import numpy as np
import shapely 
import matplotlib.pyplot as plt 
from geopy.distance import geodesic as GD
from sklearn.cluster import DBSCAN

class cluster:
    def __init__(self, points):
        self.points = points
        self.shape = self.get_shape(points)
        self.area = self.shape.area
    
    def get_shape(self, points):  
        polygons = []
        for i in range(len(points)):
            circle = shapely.Point(points[i,0], points[i,1]).buffer(.3)
            polygons.append(circle)
        union = shapely.unary_union(polygons)
        return union
 
    def plot(self):
        x,y = self.shape.exterior.xy
        plt.plot(x,y)
        plt.show()
        return 
            
    
def get_clusters(time_window_df):

    X = time_window_df["Lon"].values
    Y = time_window_df["Lat"].values
    points = np.array([[X[i],Y[i]] for i in range(len(X))])

    #Slow
    dist_matrix = distance_matrix(X,Y)
    
    db = DBSCAN(eps = 40, min_samples=1, metric="precomputed").fit(dist_matrix)
    labels = db.labels_

    n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)



    clusters = {}
    for i in range(n_clusters_):
        clusters[i] = []


    for i in range(len(labels)):
        clusters[labels[i]].append(points[i])
    
    actual_clusters = []
    for c in clusters.keys():
        aa = np.array(clusters[c])
        C = cluster(aa)
        actual_clusters.append(C)
    return actual_clusters

#Computes the distance matrix 
def distance_matrix(X,Y):
    
    dist_matrix = np.zeros((len(X), len(Y)))
    #TODO: Use scipy here 
    #https://docs.scipy.org/doc/scipy/reference/generated/scipy.spatial.distance.cdist.html#scipy.spatial.distance.cdist
    #Slow
    for i in range(len(X)):
        for j in range(len(Y)):
            p = (X[i], Y[j])
            dist_matrix[i,j] = GD((Y[i], X[i]), (Y[j], X[j])).km
            
    return dist_matrix
