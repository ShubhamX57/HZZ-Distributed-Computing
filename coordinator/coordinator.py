import os, json, time, uuid, logging
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator
import pika
import atlasopenmagic as atom

logging.basicConfig(level=logging.INFO, format="%(asctime)s [coord] %(message)s")
log = logging.getLogger(__name__)

lumi  = 36.6       # fb-1, full Run 2
skim  = "exactly4lep"
rel   = "2025e-13tev-beta"

# 2.5 GeV bins to match the notebook exactly
xlo, xhi, bw = 80, 250, 2.5
nb = int((xhi - xlo) / bw)

# sample definitions matching notebook exactly
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
                "lumi":        lumi,   # pass lumi to workers
                "color":       smp[name]["col"],
            }
            tasks.append(t)
            ch.basic_publish("", "tasks", json.dumps(t),
                             pika.BasicProperties(delivery_mode=2))
    log.info("sent %d tasks", len(tasks))
    return tasks


def get_results(ch, total):
    hists = {n: np.zeros(nb) for n in smp}
    done  = 0
    while done < total:
        mf, _, body = ch.basic_get("results", auto_ack=True)
        if body is None:
            time.sleep(0.5)
            continue
        r = json.loads(body)
        done += 1
        if r["success"]:
            hists[r["sample_name"]] += np.array(r["hist_values"])
            log.info("[%d/%d] ok  %s", done, total, r["sample_name"])
        else:
            log.error("[%d/%d] fail  %s", done, total, r["task_id"])
    return hists


def significance(hists):
    # notebook uses bins 17:20 (117.5 - 132.5 GeV with 2.5 GeV bins)
    sig_name = r"Signal ($m_H$ = 125 GeV)"
    bg_names = [k for k in smp if smp[k]["type"] == "mc" and "Signal" not in k]

    mc_tot = sum(hists[k] for k in bg_names)
    n_sig  = hists[sig_name][17:20].sum() + mc_tot[17:20].sum()
    n_bg   = mc_tot[17:20].sum()
    sig    = n_sig / np.sqrt(n_bg + 0.3*n_bg**2) if n_bg > 0 else 0.0
    log.info("n_sig=%.2f  n_bg=%.2f  sig=%.3f", n_sig, n_bg, sig)
    return n_sig, n_bg, sig


def make_plot(hists, path):
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

    # signal on top of MC stack
    sig_name = r"Signal ($m_H$ = 125 GeV)"
    ax.hist(cx, bins=edges, weights=hists[sig_name], bottom=mc_tot,
            color=smp[sig_name]["col"], label=sig_name, zorder=3)

    # stat uncertainty band — sqrt(mc_tot) Poisson, hatch drawn with black edges
    mc_err = np.sqrt(mc_tot)
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

    # no external xsec weight — the files have xsec/kfac/filteff/sum_of_weights
    # we pass lumi only; workers compute weights from in-file branches
    for name, info in data.items():
        info["xsec_weight"] = 1.0  # unused now, kept for compat

    conn = wait_rabbit(host)
    ch   = conn.channel()
    ch.queue_declare(queue="tasks",   durable=True)
    ch.queue_declare(queue="results", durable=True)
    ch.queue_purge("tasks")    # clear any leftover tasks from a previous run
    ch.queue_purge("results")

    tasks = send_tasks(ch, data)
    hists = get_results(ch, len(tasks))

    n_sig, n_bg, sig = significance(hists)

    with open(f"{outdir}/significance.txt", "w") as f:
        f.write(f"n_sig = {n_sig:.2f}\nn_bg  = {n_bg:.2f}\nsig   = {sig:.3f} sigma\n")

    make_plot(hists, f"{outdir}/HZZ_invariant_mass.png")
    log.info("all done — sig = %.3f sigma", sig)
    conn.close()


if __name__ == "__main__":
    main()
