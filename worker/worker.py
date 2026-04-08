import os, json, time, logging, traceback
import numpy as np
import uproot
import awkward as ak
import vector
import pika

logging.basicConfig(level=logging.INFO, format="%(asctime)s [worker] %(message)s")
log = logging.getLogger(__name__)

# histogram range in GeV
xlo, xhi, bw = 80, 250, 5
nb = (xhi - xlo) // bw

branches  = ["lep_pt","lep_eta","lep_phi","lep_e","lep_charge","lep_type"]
mc_br     = ["mcWeight","scaleFactor_PILEUP","scaleFactor_ELE",
              "scaleFactor_MUON","scaleFactor_LepTRIGGER"]


def avail(keys, want):
    return [k for k in want if k in keys]


def run_file(url, mc, xw):
    hist = np.zeros(nb)
    nok  = 0

    with uproot.open(url + ":analysis") as t:
        keys = t.keys()
        cols = list(set(avail(keys, branches) + (avail(keys, mc_br) if mc else [])))
        wk   = avail(keys, mc_br) if mc else []

        for ev in t.iterate(cols, library="ak", step_size=1000):
            # need at least 4 leptons
            ev = ev[ak.num(ev["lep_type"]) >= 4]
            if not len(ev): continue

            # flavour cut: 4e=44, 2e2mu=48, 4mu=52
            fs = ak.sum(ev["lep_type"][:,:4], axis=1)
            ev = ev[(fs==44)|(fs==48)|(fs==52)]
            if not len(ev): continue

            # charge cut
            ev = ev[ak.sum(ev["lep_charge"][:,:4], axis=1) == 0]
            if not len(ev): continue

            nok += len(ev)

            # pt/e are already GeV in 2025 beta release
            p = vector.zip({
                "pt":  ev["lep_pt"][:,:4],
                "eta": ev["lep_eta"][:,:4],
                "phi": ev["lep_phi"][:,:4],
                "e":   ev["lep_e"][:,:4],
            })
            m = ak.to_numpy(sum(p[:,i] for i in range(4)).M)

            if mc:
                # sherpa samples have huge raw weights so just use sign
                w = np.sign(ak.to_numpy(ev["mcWeight"]).astype(float)) if "mcWeight" in wk else np.ones(len(m))
                w[w==0] = 1.0
                w *= xw
            else:
                w = np.ones(len(m))

            h, _ = np.histogram(m, bins=nb, range=(xlo,xhi), weights=w)
            hist += h

    log.info("passed=%d  sum=%.1f", nok, hist.sum())
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
            h   = run_file(t["file_url"], mc, t["xsec_weight"])
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
