from datetime import datetime

import pandas as pd
import numpy as np
# graph algorithms
import networkx as nx

# visualisation
import matplotlib.pyplot as plt
from matplotlib.pyplot import figure
import pydot
from networkx.drawing.nx_pydot import graphviz_layout

# running scripts and catching errors
import subprocess

class Pipeline:

    def __init__(self, schedule, process, dep_on, location, status, last_run):

        self.schedule = schedule
        self.process = process
        self.dep_on = dep_on
        self.location = location
        self.status = status
        self.last_run = last_run
        self.dep_df = self.create_dep_df() 
        self.graph = self.create_graph()

        self.colour_map = []

        # empty lists to catch errors
        self.sdtout_list = []
        self.stderr_list = []

    def create_dep_df(self): # do i need to add params?

        # # modify to create dependency dataframe fit for networkx
        dep_df = self.schedule[[self.process, self.dep_on]]
        # # expand the dep_on row into mutiple rows based on ","
        dep_df = dep_df.set_index([self.process]).apply(lambda x: x.str.split(',').explode()).reset_index()
        dep_df = dep_df.dropna()
        return dep_df

    def create_graph(self):
        # create a graph object using the dataframe
        graph = nx.from_pandas_edgelist(self.dep_df,
                                        source = self.dep_on,
                                        target = self.process,
                                        create_using = nx.DiGraph)
        return graph
        

    # read a dataframe and transform into a shape that is fit for networkx
    def read_df(self):
        pass

    def set_colour_map(self):
        # colour mapping based on status for visualisation
        for index in range(len(self.schedule)):       

            if self.schedule.iloc[index][self.status] == 'no status':
                self.colour_map.append("tab:grey")
            elif self.schedule.iloc[index][self.status] == 'success':
                self.colour_map.append("tab:green")
            elif self.schedule.iloc[index][self.status] == 'error':
                self.colour_map.append("tab:red")
            elif self.schedule.iloc[index][self.status] == 'dependency failed':
                self.colour_map.append("tab:blue")
            else:
                self.colour_map.append("pink")

        

    # visualise the pipeline
    def draw(self):
        
        self.colour_map = []
        self.set_colour_map()

        # plot the new graph to visualise the status of the schedule
        pos = graphviz_layout(self.graph, prog="dot")
        plt.figure(3 ,figsize=(12, 4), dpi=80)
        nx.draw(self.graph,
                pos,
                with_labels = True,
                arrows = True,
                node_size = 1500,
                width = 1.5,
                node_color = self.colour_map,
                node_shape = "o",
                font_color = "white",
                font_family = 'calibri bold',
                bbox=dict(facecolor="black", edgecolor='black', boxstyle='round,pad=0.2'),
                horizontalalignment = 'left')

        plt.show()
        
    
    # toplogical sort
    def topological_sort(self):
        # sort the schedule dataframe using graph object
        self.schedule[self.process] = pd.Categorical(self.schedule[self.process], list(nx.topological_sort(self.graph)))
        self.schedule = self.schedule.sort_values(self.process).reset_index(drop=True)
        

    def forward_search(self, process):
        impacted_list = list(nx.dfs_tree(self.graph, source=self.schedule.loc[self.schedule[self.process] == process][self.process].values.item()))
        return impacted_list

    def backward_search(self):
        pass
    
    # run the pipeline
    def execute(self):
        # run the sorted process and catch errors
        for index in range(len(self.schedule)):

            # only run the process if it has no status
            if self.schedule.iloc[index][self.status] == "no status":
                print("running ",self.schedule.iloc[index][self.process])
                result = subprocess.run(["python", self.schedule.iloc[index]['location']], capture_output=True)
                # append the errors to the empty lists
                self.sdtout_list.append(result.stdout)
                self.stderr_list.append(result.stderr)

                # if no errors occur, change the status to success and add the current date
                if result.stderr == b'':
                    self.schedule.at[index, self.status] = 'success'
                    self.schedule.at[index, self.last_run] = datetime.date(datetime.now())

                # if an error occurs, chnage the status to error, and do not run any nodes that depend on this process, change those status to "dependency failed"
                else: 
                    self.schedule.at[index, self.status] = 'error'
                    impacted_list = list(nx.dfs_tree(self.graph, source=self.schedule.iloc[index][self.process])) #turn into function???
                    impacted_list.remove(self.schedule.iloc[index][self.process])
                    self.schedule.loc[self.schedule[self.process].isin(impacted_list), self.status] = "dependency failed"
            else:
                pass

