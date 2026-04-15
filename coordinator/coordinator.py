import os, json, time, uuid, logging
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator
import pika
import atlasopenmagic as atom
import logging
import json_logging
import threading
from prometheus_client import start_http_server, Counter, Gauge



# Configure standard logging first
logging.basicConfig(level=logging.INFO, format="%(asctime)s [app] %(message)s")



# Then initialize JSON logging (it will wrap the existing handlers)
json_logging.init_non_web(enable_json=True)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)



# Prometheus metrics
TASKS_COMPLETED = Counter('tasks_completed_total', 'Total tasks completed', ['sample_type'])
TASKS_FAILED_PERMANENT = Counter('tasks_failed_permanent_total', 'Permanently failed tasks')
TASKS_RETRIED = Counter('tasks_retried_total', 'Tasks retried')
QUEUE_LENGTH = Gauge('tasks_queue_length', 'Current pending tasks in RabbitMQ')

def start_metrics_server(port=8000):
    start_http_server(port)

threading.Thread(target=start_metrics_server, daemon=True).start()




# Constants
lumi  = 36.6       # fb-1, full Run 2
skim  = "exactly4lep"
rel   = "2025e-13tev-beta"

xlo, xhi, bw = 80, 250, 2.5
nb = int((xhi - xlo) / bw)

MAX_RETRIES = 3
BASE_RETRY_DELAY = 5  # seconds
CHECKPOINT_FILE = "/results/coordinator_checkpoint.json"




# sample definitions
smp = {
    "Data": {
        "dids": ["data"], "col": "black", "type": "data"
    },
    r"Background $Z,t\bar{t},t\bar{t}+V,VVV$": {
        "dids": [410470,410155,410218,410219,412043,
                 364243,364242,364246,364248,
                 700320,700321,700322,700323,700324,700325],
        "col": "#6b59d3", "type": "mc"
    },
    r"Background $ZZ^{*}$": {
        "dids": [700600], "col": "#ff0000", "type": "mc"
    },
    r"Signal ($m_H$ = 125 GeV)": {
        "dids": [345060,346228,346310,346311,346312,346340,346341,346342],
        "col": "#00cdff", "type": "mc"
    },
}





def wait_rabbit(host):
    for i in range(15):
        try:
            c = pika.BlockingConnection(pika.ConnectionParameters(
                host=host, heartbeat=600, blocked_connection_timeout=300))
            log.info("rabbit connected")
            return c
        except Exception as e:
            log.warning("attempt %d: %s", i+1, e)
            time.sleep(5)
    raise RuntimeError("can't connect to rabbit")



def send_tasks(ch, data):
    tasks = []
    for name, info in data.items():
        for url in info.get("list", []):
            t = {
                "task_id":     str(uuid.uuid4()),
                "sample_name": name,
                "sample_type": smp[name]["type"],
                "file_url":    url,
                "lumi":        lumi,
                "color":       smp[name]["col"],
                "retries":     0,
            }
            tasks.append(t)
            ch.basic_publish("", "tasks", json.dumps(t),
                             pika.BasicProperties(delivery_mode=2))
    log.info("sent %d tasks", len(tasks))
    QUEUE_LENGTH.set(len(tasks))
    return tasks



def save_checkpoint(hists, sq_weights, processed_ids):
    state = {
        "hists": {k: v.tolist() for k, v in hists.items()},
        "sq_weights": {k: v.tolist() for k, v in sq_weights.items()},
        "processed_ids": list(processed_ids),
    }
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(state, f)



def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return None
    with open(CHECKPOINT_FILE) as f:
        state = json.load(f)
    hists = {k: np.array(v) for k, v in state["hists"].items()}
    sq_weights = {k: np.array(v) for k, v in state["sq_weights"].items()}
    processed = set(state["processed_ids"])
    return hists, sq_weights, processed



def get_results(ch, total):
    # Attempt to load checkpoint
    cp = load_checkpoint()
    if cp:
        hists, sq_weights, processed_ids = cp
        log.info("Resumed from checkpoint with %d tasks already processed", len(processed_ids))
    else:
        hists = {n: np.zeros(nb) for n in smp}
        sq_weights = {n: np.zeros(nb) for n in smp if smp[n]["type"] == "mc"}
        processed_ids = set()

    done = len(processed_ids)
    failed_permanently = 0
    last_checkpoint = done


    while done + failed_permanently < total:
        mf, _, body = ch.basic_get("results", auto_ack=True)
        if body is None:
            time.sleep(0.5)
            continue

        r = json.loads(body)
        task_id = r["task_id"]

        if task_id in processed_ids:
            continue  # duplicate (e.g., after restart)

        if r["success"]:
            done += 1
            processed_ids.add(task_id)
            hists[r["sample_name"]] += np.array(r["hist_values"])
            if r["sample_type"] == "mc" and "hist_sqweights" in r:
                sq_weights[r["sample_name"]] += np.array(r["hist_sqweights"])
            log.info("[%d/%d] ok  %s", done, total, r["sample_name"])
            TASKS_COMPLETED.labels(sample_type=r["sample_type"]).inc()
        else:
            retries = r.get("retries", 0)
            if retries < MAX_RETRIES:
                delay = BASE_RETRY_DELAY * (2 ** retries)
                log.warning("Task %s failed (attempt %d/%d). Retrying in %ds...",
                            task_id[:8], retries+1, MAX_RETRIES+1, delay)
                time.sleep(delay)
                r["retries"] = retries + 1
                ch.basic_publish("", "tasks", json.dumps(r),
                                 pika.BasicProperties(delivery_mode=2))
                TASKS_RETRIED.inc()
            else:
                failed_permanently += 1
                log.error("Task %s permanently failed after %d retries.",
                          task_id[:8], MAX_RETRIES)
                TASKS_FAILED_PERMANENT.inc()
                # Send to dead letter exchange
                ch.basic_publish(exchange="dlx", routing_key="", body=json.dumps(r),
                                 properties=pika.BasicProperties(delivery_mode=2))

        QUEUE_LENGTH.set(total - done - failed_permanently)


        # Checkpoint every 5 completions
        if done - last_checkpoint >= 5:
            save_checkpoint(hists, sq_weights, processed_ids)
            last_checkpoint = done


    save_checkpoint(hists, sq_weights, processed_ids)
    log.info("Completed: %d succeeded, %d permanently failed.", done, failed_permanently)
    return hists, sq_weights



def significance(hists):
    sig_name = r"Signal ($m_H$ = 125 GeV)"
    bg_names = [k for k in smp if smp[k]["type"] == "mc" and "Signal" not in k]

    mc_tot = sum(hists[k] for k in bg_names)
    n_sig  = hists[sig_name][17:20].sum() + mc_tot[17:20].sum()
    n_bg   = mc_tot[17:20].sum()
    sig    = n_sig / np.sqrt(n_bg + 0.3*n_bg**2) if n_bg > 0 else 0.0
    log.info("n_sig=%.2f  n_bg=%.2f  sig=%.3f", n_sig, n_bg, sig)
    return n_sig, n_bg, sig



def make_plot(hists, sq_weights, path):
    edges = np.arange(xlo, xhi + bw, bw)
    cx    = edges[:-1] + bw/2

    fig, ax = plt.subplots(figsize=(12, 8))


    # data
    data_y     = hists["Data"]
    data_err   = np.sqrt(data_y)
    ax.errorbar(cx, data_y, yerr=data_err, fmt="ko", ms=4, label="Data", zorder=5)


    # MC backgrounds stacked
    bg_names = [k for k in smp if smp[k]["type"] == "mc" and "Signal" not in k]
    mc_x     = [hists[k] for k in bg_names]
    mc_cols  = [smp[k]["col"] for k in bg_names]
    mc_h     = ax.hist([cx]*len(mc_x), bins=edges, weights=mc_x,
                       stacked=True, color=mc_cols, label=bg_names)
    mc_tot   = mc_h[0][-1]



    # signal on top
    sig_name = r"Signal ($m_H$ = 125 GeV)"
    ax.hist(cx, bins=edges, weights=hists[sig_name], bottom=mc_tot,
            color=smp[sig_name]["col"], label=sig_name, zorder=3)



    # Stat uncertainty band
    total_sq_weights = np.zeros(nb)
    for k in bg_names:
        total_sq_weights += sq_weights[k]
    mc_err = np.sqrt(total_sq_weights)
    ax.bar(cx, 2*mc_err, bottom=mc_tot - mc_err,
           width=bw, color="none", edgecolor="black", linewidth=0,
           hatch="////", alpha=0.5, label="Stat. Unc.", zorder=4)



    ax.set_xlim(xlo, xhi)
    ax.set_ylim(bottom=0)
    ax.xaxis.set_minor_locator(AutoMinorLocator())
    ax.yaxis.set_minor_locator(AutoMinorLocator())
    ax.tick_params(which="both", direction="in", top=True, right=True)
    ax.set_xlabel(r"4-lepton invariant mass $m_{4\ell}$ [GeV]",
                  fontsize=13, x=1, ha="right")
    ax.set_ylabel(f"Events / {bw} GeV", y=1, ha="right")


    for y, s, kw in [
        (0.95, "ATLAS Open Data",  {"fontsize":14}),
        (0.89, "for education",    {"fontsize":11,"style":"italic"}),
        (0.83, r"$\sqrt{s}=13\ \mathrm{TeV},\ \int\mathcal{L}\,dt=36.6\ \mathrm{fb}^{-1}$", {"fontsize":12}),
        (0.77, r"$H \rightarrow ZZ^* \rightarrow 4\ell$", {"fontsize":13}),
    ]:
        ax.text(0.05, y, s, transform=ax.transAxes, va="top", **kw)

    ax.legend(frameon=False, fontsize=10, loc="upper right")
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    log.info("plot saved: %s", path)





def main():
    host   = os.environ.get("RABBITMQ_HOST", "localhost")
    outdir = os.environ.get("RESULTS_DIR", "/results")
    os.makedirs(outdir, exist_ok=True)


    log.info("loading %s", rel)
    atom.set_release(rel)
    data = atom.build_dataset(smp, skim=skim, protocol="https", cache=True)



    for name, info in data.items():
        info["xsec_weight"] = 1.0


    conn = wait_rabbit(host)
    ch   = conn.channel()



    # Setup Dead Letter Exchange
    ch.exchange_declare(exchange="dlx", exchange_type="direct", durable=True)
    ch.queue_declare(queue="dead_letters", durable=True)
    ch.queue_bind(exchange="dlx", queue="dead_letters", routing_key="")


    ch.queue_declare(queue="tasks",   durable=True)
    ch.queue_declare(queue="results", durable=True)
    ch.queue_purge("tasks")
    ch.queue_purge("results")


    tasks = send_tasks(ch, data)
    hists, sq_weights = get_results(ch, len(tasks))

    n_sig, n_bg, sig = significance(hists)

    with open(f"{outdir}/significance.txt", "w") as f:
        f.write(f"n_sig = {n_sig:.2f}\nn_bg  = {n_bg:.2f}\nsig   = {sig:.3f} sigma\n")



    make_plot(hists, sq_weights, f"{outdir}/HZZ_invariant_mass.png")
    log.info("all done — sig = %.3f sigma", sig)
    conn.close()



if __name__ == "__main__":
    main()
