from cluster import get_clusters
import numpy as np

class time_window:
    def __init__(self, storm_report_df, rrfs_surrogate_reports_dict):
        
        self.storm_reports_df = storm_report_df
        self.rrfs_surrogate_reports_dict = rrfs_surrogate_reports_dict
        self.storm_report_clusters = get_clusters(storm_report_df)
        self.surrogate_report_clusters = self.get_surrogate_clusters(rrfs_surrogate_reports_dict)
        
    def analysis(self, variable=False):
        if variable:
            if variable not in self.rrfs_surrogate_reports_dict.keys():
                raise Exception(f"{variable} not supported")
            else :
                return self.analysis_var(variable)
        else :
            analysis_dict = {}
            for var in self.surrogate_report_clusters.keys():
                analysis_dict[var] =self.analysis_var(var)
            return analysis_dict
    
    def analysis_var(self, var):
        surrogate_clusters = self.surrogate_report_clusters[var]
        storm_report_clusters = self.storm_report_clusters
        # print(f"surrogate neighborhoods {len(surrogate_slices)}")
        # print(f"storm report neighborhoods {len(storm_report_slices)}")

        #Hits and misses
        overlaps = [0]*len(storm_report_clusters)
        for i in range(len(storm_report_clusters)):
            for j in range(len(surrogate_clusters)):
                if storm_report_clusters[i].shape.intersects(surrogate_clusters[j].shape):
                    overlaps[i] = 1

        hits = np.array(overlaps).sum()
        misses = len(storm_report_clusters) - hits
        #False alarms
        # overlaps = [0]*len(surrogate_slices)
        # count = 0
        overlaps = [0]*len(surrogate_clusters)
        for i in range(len(surrogate_clusters)):
            for j in range(len(storm_report_clusters)):
                if not surrogate_clusters[i].shape.intersects(storm_report_clusters[j].shape):
                    overlaps[i] = 1

        false_alarms = np.array(overlaps).sum()
        POD = hits / (hits + misses)
        FAR = false_alarms / (false_alarms + hits)
        bias = (hits + false_alarms)/ (hits + misses)
        CSI = hits /(hits + false_alarms + misses)
        #CSI
        #FAR
        return {"window_analysis": {"POD":POD, "FAR":FAR, "bias":bias, "CSI": CSI}, "data": {"hits": hits, "misses": misses, "false_alarms": false_alarms}}
 
    
    def get_surrogate_clusters(self, rrfs_surrogate_reports_dict):
        clusters = {}
        for var in rrfs_surrogate_reports_dict.keys():
            clusters[var] = get_clusters(rrfs_surrogate_reports_dict[var])
        return clusters