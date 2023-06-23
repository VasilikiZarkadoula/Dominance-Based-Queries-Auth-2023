import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
import pandas as pd

np.set_printoptions(suppress=True)

def correlated(dim, n, anti=False):
    mu = np.zeros(dim)
    r = np.ones((dim, dim))
    for i in range(dim):
        r[i][i] = 1.5
    l = np.random.multivariate_normal(mu, r, size=n)
    for d in range(l.shape[1]):
        col = l[:, d]
        l[:, d] = (col - col.min()) / (col.max() - col.min())
    if anti:
        l[:, 0] = l[:, 0].max() - l[:, 0]
    minmax = MinMaxScaler()
    return minmax.fit_transform(np.array(l))

def anticorrelated(dim, n):
    return correlated(dim, n, True)

def gaussian(dim, n):
    points = []
    for i in range(n):
        point = []
        for d in range(dim):
            point.append(np.random.normal(0, 1))
        points.append(point)
    minmax = MinMaxScaler()
    return minmax.fit_transform(np.array(points))

def uniform(dim, n):
    points = []
    for i in range(n):
        point = []
        for d in range(dim):
            point.append(np.random.uniform(0, 1))
        points.append(point)
    minmax = MinMaxScaler()
    return minmax.fit_transform(np.array(points))

def write_to_file(points, distribution):
    np.savetxt(distribution+".csv", points, delimiter=",", fmt='%f')

#l = anticorrelated(5, 10000)
#plt.scatter(l[:, 2], l[:, 1])
#plt.show()

def view(dist):
    data = pd.read_csv(dist+'.csv', header=None).to_numpy()
    print(data.shape)
    plt.scatter(data[:, 0], data[:, 1])
    plt.show()

write_to_file(correlated(4, 32000000), "correlated_dim4_nsamples_32M")
#write_to_file(anticorrelated(2, 1000), "anticorrelated")
#write_to_file(gaussian(5, 1000000), "gaussian")
#write_to_file(uniform(5, 1000000), "uniform")
#view("uniform")
