import os, json, time, logging, traceback
import numpy as np
import uproot
import awkward as ak
import vector
import pika
import logging
import json_logging


# Configure standard logging first
logging.basicConfig(level=logging.INFO, format="%(asctime)s [app] %(message)s")



# Then initialize JSON logging (it will wrap the existing handlers)
json_logging.init_non_web(enable_json=True)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)



xlo, xhi, bw = 80, 250, 2.5
nb = int((xhi - xlo) / bw)



base_vars = ["lep_pt","lep_eta","lep_phi","lep_e","lep_charge","lep_type",
             "trigE","trigM","lep_isTrigMatched","lep_isLooseID",
             "lep_isMediumID","lep_isLooseIso"]


mc_vars = ["filteff","kfac","xsec","mcWeight",
           "ScaleFactor_PILEUP","ScaleFactor_ELE",
           "ScaleFactor_MUON","ScaleFactor_LepTRIGGER",
           "sum_of_weights"]



def avail(keys, want):
    return [k for k in want if k in keys]



def run_file(url, mc, lumi):
    hist = np.zeros(nb)
    hist_sq = np.zeros(nb)
    nok  = 0


    with uproot.open(url + ":analysis", timeout=600) as t:
        keys  = t.keys()
        bvars = avail(keys, base_vars)
        wvars = avail(keys, mc_vars) if mc else []
        cols  = list(set(bvars + wvars))


        if mc and "sum_of_weights" in wvars:
            sumw_global = t["sum_of_weights"].array()[0]
        else:
            sumw_global = 1.0


        for ev in t.iterate(cols, library="ak", step_size=1000):
            if "trigE" in keys and "trigM" in keys:
                ev = ev[ev.trigE | ev.trigM]
            if not len(ev): continue

            if "lep_isTrigMatched" in keys:
                ev = ev[ak.sum(ev.lep_isTrigMatched, axis=1) >= 1]
            if not len(ev): continue


            ev = ev[ev["lep_pt"][:,0] > 20]
            if not len(ev): continue
            ev = ev[ev["lep_pt"][:,1] > 15]
            if not len(ev): continue
            ev = ev[ev["lep_pt"][:,2] > 10]
            if not len(ev): continue


            if all(b in keys for b in ["lep_isLooseID","lep_isMediumID","lep_isLooseIso","lep_type"]):
                pid = ev.lep_type
                eid = ev.lep_isLooseID
                mid = ev.lep_isMediumID
                iso = ev.lep_isLooseIso
                mask = ak.sum(((pid==13)&mid&iso)|((pid==11)&eid&iso), axis=1) == 4
                ev = ev[mask]
            if not len(ev): continue

            lt  = ev["lep_type"]
            fs  = lt[:,0]+lt[:,1]+lt[:,2]+lt[:,3]
            ev  = ev[(fs==44)|(fs==48)|(fs==52)]
            if not len(ev): continue

            ev = ev[ev["lep_charge"][:,0]+ev["lep_charge"][:,1]+
                    ev["lep_charge"][:,2]+ev["lep_charge"][:,3] == 0]
            if not len(ev): continue

            nok += len(ev)

            p = vector.zip({"pt":ev["lep_pt"][:,:4],"eta":ev["lep_eta"][:,:4],
                            "phi":ev["lep_phi"][:,:4],"E":ev["lep_e"][:,:4]})
            m = ak.to_numpy((p[:,0]+p[:,1]+p[:,2]+p[:,3]).M)

            if mc:
                w = lumi * 1000.0 / sumw_global
                for var in ["filteff","kfac","xsec","mcWeight",
                            "ScaleFactor_PILEUP","ScaleFactor_ELE",
                            "ScaleFactor_MUON","ScaleFactor_LepTRIGGER"]:
                    if var in wvars:
                        w = w * np.abs(ak.to_numpy(ev[var]).astype(float))
            else:
                w = np.ones(len(m))

            h, _ = np.histogram(m, bins=nb, range=(xlo, xhi), weights=w)
            hist += h
            if mc:
                h_sq, _ = np.histogram(m, bins=nb, range=(xlo, xhi), weights=w**2)
                hist_sq += h_sq

    log.info("passed=%d  sum=%.2f", nok, hist.sum())
    return hist, hist_sq






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
            h, h_sq = run_file(t["file_url"], mc, t["lumi"])
            out = {**t, "hist_values": h.tolist(), "success": True, "error": None}
            if mc:
                out["hist_sqweights"] = h_sq.tolist()
        except Exception:
            err = traceback.format_exc()
            log.error("failed:\n%s", err)
            out = {**t, "hist_values": np.zeros(nb).tolist(), "success": False, "error": err}
            if mc:
                out["hist_sqweights"] = np.zeros(nb).tolist()

        ch.basic_publish("", "results", json.dumps(out),
                         pika.BasicProperties(delivery_mode=2))
        ch.basic_ack(meth.delivery_tag)

    ch.basic_consume("tasks", cb)
    log.info("waiting for tasks ...")
    ch.start_consuming()




if __name__ == "__main__":
    main()
