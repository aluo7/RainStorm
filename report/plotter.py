import matplotlib.pyplot as plt
import numpy as np

experiments = [
    "Experiment 1",
    "Experiment 2",
    "Experiment 3",
    "Experiment 4",
]

spark_data = [
    [880, 943, 712],
    [2917, 2446, 2814],
    [819, 541, 767],
    [620, 694, 892],
]

rainstorm_data = [
    [757, 802, 697],
    [410, 537, 373],
    [713, 762, 622],
    [705, 641, 732],
]

spark_avg = [np.mean(vals) for vals in spark_data]
spark_std = [np.std(vals) for vals in spark_data]

rain_avg = [np.mean(vals) for vals in rainstorm_data]
rain_std = [np.std(vals) for vals in rainstorm_data]

x = np.arange(len(experiments))
width = 0.35

fig, ax = plt.subplots(figsize=(10, 6))

ax.bar(
    x - width / 2,
    spark_avg,
    width,
    yerr=spark_std,
    capsize=5,
    label="Spark",
    color="#1f77b4",
)

ax.bar(
    x + width / 2,
    rain_avg,
    width,
    yerr=rain_std,
    capsize=5,
    label="Rainstorm",
    color="#ff7f0e",
)

ax.set_ylabel("Output Rate (tuples/second)")
ax.set_xlabel("Experiment")
ax.set_title("Experimental Results")
ax.set_xticks(x)
ax.set_xticklabels(experiments, rotation=20)
ax.legend()

plt.tight_layout()
plt.show()
