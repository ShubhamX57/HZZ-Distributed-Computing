import os, json, time, logging, traceback
import numpy as np
import uproot
import awkward as ak
import vector
import pika

logging.basicConfig(level=logging.INFO, format="%(asctime)s [worker] %(message)s")
log = logging.getLogger(__name__)

# histogram range — 2.5 GeV bins matching the notebook
xlo, xhi, bw = 80, 250, 2.5
nb = int((xhi - xlo) / bw)

# branches to read (notebook variables)
base_vars = ["lep_pt","lep_eta","lep_phi","lep_e","lep_charge","lep_type",
             "trigE","trigM","lep_isTrigMatched","lep_isLooseID",
             "lep_isMediumID","lep_isLooseIso"]

# weight branches stored in the MC files (notebook weight_variables)
mc_vars = ["filteff","kfac","xsec","mcWeight",
           "ScaleFactor_PILEUP","ScaleFactor_ELE",
           "ScaleFactor_MUON","ScaleFactor_LepTRIGGER",
           "sum_of_weights"]


def avail(keys, want):
    return [k for k in want if k in keys]


def run_file(url, mc, lumi):
    hist = np.zeros(nb)
    nok  = 0

    with uproot.open(url + ":analysis") as t:
        keys  = t.keys()
        bvars = avail(keys, base_vars)
        wvars = avail(keys, mc_vars) if mc else []
        cols  = list(set(bvars + wvars))

        for ev in t.iterate(cols, library="ak", step_size=1000):
            # trigger cuts (if branches present)
            if "trigE" in keys and "trigM" in keys:
                ev = ev[ev.trigE | ev.trigM]
            if not len(ev): continue

            if "lep_isTrigMatched" in keys:
                ev = ev[ak.sum(ev.lep_isTrigMatched, axis=1) >= 1]
            if not len(ev): continue

            # pT cuts on leading three leptons (notebook: >20, >15, >10 GeV)
            ev = ev[ev["lep_pt"][:,0] > 20]
            if not len(ev): continue
            ev = ev[ev["lep_pt"][:,1] > 15]
            if not len(ev): continue
            ev = ev[ev["lep_pt"][:,2] > 10]
            if not len(ev): continue

            # ID / isolation cut
            if all(b in keys for b in ["lep_isLooseID","lep_isMediumID","lep_isLooseIso","lep_type"]):
                pid = ev.lep_type
                eid = ev.lep_isLooseID
                mid = ev.lep_isMediumID
                iso = ev.lep_isLooseIso
                mask = ak.sum(((pid==13)&mid&iso)|((pid==11)&eid&iso), axis=1) == 4
                ev = ev[mask]
            if not len(ev): continue

            # flavour cut: 4e=44, 2e2mu=48, 4mu=52
            lt  = ev["lep_type"]
            fs  = lt[:,0]+lt[:,1]+lt[:,2]+lt[:,3]
            ev  = ev[(fs==44)|(fs==48)|(fs==52)]
            if not len(ev): continue

            # charge cut
            ev = ev[ev["lep_charge"][:,0]+ev["lep_charge"][:,1]+
                    ev["lep_charge"][:,2]+ev["lep_charge"][:,3] == 0]
            if not len(ev): continue

            nok += len(ev)

            # 4-lepton invariant mass (pt/e already in GeV)
            p = vector.zip({"pt":ev["lep_pt"][:,:4],"eta":ev["lep_eta"][:,:4],
                            "phi":ev["lep_phi"][:,:4],"E":ev["lep_e"][:,:4]})
            m = ak.to_numpy((p[:,0]+p[:,1]+p[:,2]+p[:,3]).M)

            # weights
            if mc:
                # full notebook formula: lumi*1000/sum_of_weights * prod(abs(vars))
                sw = ak.to_numpy(ev["sum_of_weights"]).astype(float) if "sum_of_weights" in wvars else np.ones(len(m))
                w  = lumi * 1000.0 / sw
                for var in ["filteff","kfac","xsec","mcWeight",
                            "ScaleFactor_PILEUP","ScaleFactor_ELE",
                            "ScaleFactor_MUON","ScaleFactor_LepTRIGGER"]:
                    if var in wvars:
                        w = w * np.abs(ak.to_numpy(ev[var]).astype(float))
            else:
                w = np.ones(len(m))

            h, _ = np.histogram(m, bins=nb, range=(xlo, xhi), weights=w)
            hist += h

    log.info("passed=%d  sum=%.2f", nok, hist.sum())
    return hist


def connect(host):
    for i in range(12):
        try:
            c = pika.BlockingConnection(pika.ConnectionParameters(
                host=host, heartbeat=600, blocked_connection_timeout=300))
            log.info("connected")
            return c
        except Exception as e:
            log.warning("retry %d: %s", i+1, e)
            time.sleep(5)
    raise RuntimeError("can't reach rabbitmq")


def main():
    host = os.environ.get("RABBITMQ_HOST", "localhost")
    conn = connect(host)
    ch   = conn.channel()
    ch.queue_declare(queue="tasks",   durable=True)
    ch.queue_declare(queue="results", durable=True)
    ch.basic_qos(prefetch_count=1)

    def cb(ch, meth, props, body):
        t  = json.loads(body)
        mc = t["sample_type"] == "mc"
        log.info("got %s | %s", t["task_id"][:8], t["sample_name"])
        try:
            h   = run_file(t["file_url"], mc, t["lumi"])
            out = {**t, "hist_values": h.tolist(), "success": True, "error": None}
        except Exception:
            err = traceback.format_exc()
            log.error("failed:\n%s", err)
            out = {**t, "hist_values": np.zeros(nb).tolist(), "success": False, "error": err}

        ch.basic_publish("", "results", json.dumps(out),
                         pika.BasicProperties(delivery_mode=2))
        ch.basic_ack(meth.delivery_tag)

    ch.basic_consume("tasks", cb)
    log.info("waiting for tasks ...")
    ch.start_consuming()


if __name__ == "__main__":
    main()
