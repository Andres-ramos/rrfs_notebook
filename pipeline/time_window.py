from pipeline.cluster import get_clusters
import numpy as np

class time_window:
    def __init__(self, storm_report_df, rrfs_surrogate_reports_dict):
        
        self.storm_reports_df = storm_report_df
        self.rrfs_surrogate_reports_dict = rrfs_surrogate_reports_dict
        self.storm_report_clusters = get_clusters(self.storm_reports_df)
        self.surrogate_report_clusters = self.get_surrogate_clusters(rrfs_surrogate_reports_dict)
        
    def analysis(self, variable=False):
        # for var in self.surrogate_report_clusters.keys():
        #     print(var, len(self.surrogate_report_clusters[var]))
        if variable:
            if variable not in self.rrfs_surrogate_reports_dict.keys():
                raise Exception(f"{variable} not supported")
            else :
                return self.analysis_var(variable)
        else :
            analysis_dict = {}
            for var in self.surrogate_report_clusters.keys():
                analysis_dict[var] =self.analysis_var(var)
                # print("analysis dict", analysis_dict)
            return analysis_dict
    
    def analysis_var(self, var):
        if self.storm_report_clusters == 0:

            #If rrfs surrogates, all false alarms
            if self.surrogate_report_clusters[var] == 0:
                return self.result_dict(hits=0, misses=0, false_alarms=0)
            else :
                false_alarms = len(self.surrogate_report_clusters[var])
                # print(f"# false alarms {false_alarms}")
                # print("result",self.result_dict(hits=0, misses=0, false_alarms=false_alarms))
                return self.result_dict(hits=0, misses=0, false_alarms=false_alarms)
        
        elif self.surrogate_report_clusters[var] == 0:
            #If it entered here, storm reports != 0 and there are no surrogates
            #All misses
            misses = len(self.storm_report_clusters)
            return self.result_dict(hits=0, misses=misses, false_alarms=0)
        else :
                
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
            overlaps = [0]*len(surrogate_clusters)
            for i in range(len(surrogate_clusters)):
                for j in range(len(storm_report_clusters)):
                    if not surrogate_clusters[i].shape.intersects(storm_report_clusters[j].shape):
                        overlaps[i] = 1

            false_alarms = np.array(overlaps).sum()

            return self.result_dict(hits, misses, false_alarms)
 
    
    def get_surrogate_clusters(self, rrfs_surrogate_reports_dict):
        clusters = {}
        for var in rrfs_surrogate_reports_dict.keys():
            clusters[var] = get_clusters(rrfs_surrogate_reports_dict[var])
        return clusters
    
    def compute_statistics(self,hits, misses, false_alarms):
        try :
            POD = hits / (hits + misses)
        except:
            POD = np.nan
        try :
            FAR = false_alarms / (false_alarms + hits)
        except :
            FAR = np.nan
        try :
            bias = (hits + false_alarms)/ (hits + misses)
        except: 
            bias = np.nan
        try :
            CSI = hits /(hits + false_alarms + misses)
        except :
            CSI = np.nan

        return POD, FAR, bias, CSI
    
    def result_dict(self, hits, misses, false_alarms):
        POD, FAR, bias, CSI = self.compute_statistics(hits, misses, false_alarms)
        return {
            "window_analysis": {
                "POD":POD, "FAR":FAR, "bias":bias, "CSI": CSI
                }, 
            "data": {
                "hits": hits, 
                "misses": misses, 
                "false_alarms": false_alarms
                }
            }
