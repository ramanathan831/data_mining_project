from matplotlib import pyplot as plt

xBefore = [0.1, 0.2, 0.3, 0.4]
yBefore = [1, 4, 9, 16]

xAfter = [0.1, 0.2, 0.3, 0.4]
yAfter = [1, 2, 3, 4]

file_object = open("")
lines = file_object.readlines()

for line in lines:
    print(line)

plt.plot(xAfter, yAfter, label='During COVID-19')
plt.plot(xBefore, yBefore, label='Before COVID-19')
plt.legend()
plt.show()