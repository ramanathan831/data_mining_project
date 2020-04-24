import matplotlib
import matplotlib.pyplot as plt
import numpy as np


# labels = ['Content\nBased', 'Collaborative', 'Hybrid_1\nContent-Based\n+ Sentiment', 'Hybrid_2\nCollaborative\n+ Sentiment', 'All Combined']
labels = ['Generic', 'With User\n Reviews']

RMSE = [88.7, 87.0]
MAE = [88.93, 87.3]

x = np.arange(len(labels))  # the label locations
width = 0.35  # the width of the bars

fig, ax = plt.subplots()
rects1 = ax.bar(x - width/2, RMSE, width, label='Precision')
rects2 = ax.bar(x + width/2, MAE, width, label='Recall')



# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('Ratings')
ax.set_title('Collaborative')
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()


def autolabel(rects):
    """Attach a text label above each bar in *rects*, displaying its height."""
    for rect in rects:
        height = rect.get_height()
        ax.annotate('{}'.format(height),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')


autolabel(rects1)
autolabel(rects2)

fig.tight_layout()

plt.show()
