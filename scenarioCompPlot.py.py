"""Module to import data of a comparison of nestor scenarios"""

from asyncio import protocols
from turtle import color
from random import randint
import matplotlib.cm as cm
from glob import glob
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import os
from scenarioCompData import DataExtractionScenarioComparison
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick



colors_=['white','sandybrown','gray','yellow','blue','green','purple', 'red','salmon', 'fuchsia', 'lime', 'orangered', 'navy', 'olive',  'teal', 'Silver', 'Coral', 'brown','Lavender', 'Turquoise', 'peru'] #'hotpink','pink'
width_ = 0.15        

class PlotScenarioComparison():
    def __init__(self, path_list, fig_save_path=None,name=None):
        self.path_list = path_list

        # checks
        if not isinstance(path_list, list):
            raise ValueError("path_list needs to be a list of paths")
        for path in path_list:
            if not os.path.isfile(path):
                raise ValueError(f"Path '{path}' is not leading to a file")

        # raise NotImplementedError(
        #     "A scenario comparison is not yet implemented")
        if name is None:
            self.name = os.path.basename(path)
        else:
            self.name = name
        if fig_save_path == None:
            path = path = os.path.dirname(os.path.abspath(__file__))
            self.fig_save_path = os.path.join(
                path, "plots", self.name.replace(".xlsx", ""))
            if not os.path.isdir(self.fig_save_path):
                os.mkdir(self.fig_save_path)
        else:
            raise NotImplementedError()
    
    def fig_installed_cap(self):
        data = DataExtractionScenarioComparison(self.path_list).data_installed_cap()
        print('installed_cap',data)
        sum_data,percent_sum =[],[]  
        for t in data:
            sum_data.append(t.sum(axis=1))
            percent_sum.append(t.sum(axis=1).pct_change())

            print('percentage change in Installed capacities',t.pct_change())
        a,b = pd.concat(sum_data,axis=1), pd.concat(percent_sum,axis=1)
        print(a.iloc[:,::-1], '\n', b.iloc[:,::-1])

        #labelling the plots by cases
        cases =[]
        for x in self.path_list:
            case= []
            cases.append(case)
            fs = os.path.dirname(x) + '/input/raw_input_data/renewables/*/*'
            for i in glob(fs):
                j = (os.path.basename(i))
                case.append(j)
        for p in range(len(cases)):
            print(cases[p], '\n', end =" ")
    
        fig, ax = plt.subplots(figsize =(12, 8))
        for count,dat in enumerate(data):
       
            dat.plot(kind="bar", stacked=True, edgecolor='black', width=width_, 
                        ax=ax, position=count) 
        ax.set_ylabel('Solar & Wind Energy Installed capacities [GW]')
        ax.set_title(f'scenario comparison: \n{cases[0]}\n {cases[1]}\n {cases[2]}\n {cases[3]}\n {cases[4]}')
        ax.set_xlim(right=len(dat)-0.5)
        ax.legend(dat.columns, loc='center left', bbox_to_anchor=(1, 0.5))
        fig.tight_layout()

        save_path = os.path.join(
            self.fig_save_path, f'multi_comparison_installed_cap.png')
        plt.savefig(save_path)

    def fig_Energy_demand(self):
        data = DataExtractionScenarioComparison(self.path_list).data_electricity_demand()
        print('fig_Energy_demand',data)
        sum_data,percent_sum =[],[]  
        for t in data:
            sum_data.append(t.sum(axis=1))
            percent_sum.append(t.sum(axis=1).pct_change())

            print('percentage change in Energy_demand',t.pct_change())
        a,b = pd.concat(sum_data,axis=1), pd.concat(percent_sum,axis=1)
        print(a,b)

        #labelling the plots by cases
        cases =[]
        for x in self.path_list:
            case= []
            cases.append(case)
            fs = os.path.dirname(x) + '/input/raw_input_data/renewables/*/*'
            for i in glob(fs):
                j = (os.path.basename(i))
                case.append(j)
        for p in range(len(cases)):
            print(cases[p], '\n', end =" ")
    
        fig, ax = plt.subplots(figsize =(12, 8))
        for count,dat in enumerate(data):
       
            dat.plot(kind="bar", stacked=True, edgecolor='black', width=width_, 
                        ax=ax, position=count) 
        ax.set_ylabel('Energy Demand[TWH]')
        ax.set_title(f'scenario comparison: \n{cases[0]}\n {cases[1]}\n {cases[2]}\n {cases[3]}\n {cases[4]}')
        ax.set_xlim(right=len(dat)-0.5)
        ax.legend(dat.columns, loc='center left', bbox_to_anchor=(1, 0.5))
        fig.tight_layout()

        save_path = os.path.join(
            self.fig_save_path, f'multi_Energy_Demand.png')
        plt.savefig(save_path)

    def fig_cost(self):
        data = DataExtractionScenarioComparison(self.path_list).data_cost()
        print('Cost',data)
        sum_data,percent_sum =[],[]  
        for t in data:
            sum_data.append(t.sum(axis=1))
            percent_sum.append(t.sum(axis=1).pct_change())

            # print('percentage change in Cost',t.pct_change())
        a,b = pd.concat(sum_data,axis=1), pd.concat(percent_sum,axis=1)
        print(a.iloc[:,::-1], '\n','cummulative sum',a.cumsum().iloc[:,::-1], b.iloc[:,::-1])
        
        cases =[]
        for x in self.path_list:
            case= []
            cases.append(case)
            fs = os.path.dirname(x) + '/input/raw_input_data/renewables/*/*'
            for i in glob(fs):
                j = (os.path.basename(i))
                case.append(j)
        
    
        fig, ax = plt.subplots(figsize =(12, 8))
        for count,dat in enumerate(data):
       
            dat.plot(kind="bar", stacked=True, edgecolor='black', width=width_, 
                        ax=ax, position=count) 
        ax.set_ylabel('Cost Billion €/a')
        ax.set_title(f'scenario comparison: \n{cases[0]}\n {cases[1]}\n {cases[2]}\n {cases[3]}\n {cases[4]}')
        ax.set_xlim(right=len(dat)-0.5)
        ax.legend(dat.columns,loc='center left', bbox_to_anchor=(1, 0.5))
        fig.tight_layout()

        save_path = os.path.join(
            self.fig_save_path, f'multi_comparison_cost.png')
        plt.savefig(save_path)

    def fig_CO2(self):
        data = DataExtractionScenarioComparison(self.path_list).data_CO2()
        print('Emissions')
        print('Emissions',data)
        sum_data,percent_sum =[],[]  
        for t in data:
            sum_data.append(t.sum(axis=1))
            percent_sum.append(t.sum(axis=1).pct_change())

            print('percentage change in Emissions',t.pct_change())
        a,b = pd.concat(sum_data,axis=1), pd.concat(percent_sum,axis=1)
        print(a.iloc[:,::-1], '\n', b.iloc[:,::-1])
        
        cases =[]
        for x in self.path_list:
            case= []
            cases.append(case)
            fs = os.path.dirname(x) + '/input/raw_input_data/renewables/*/*'
            for i in glob(fs):
                j = (os.path.basename(i))
                case.append(j)
        
    
        fig, ax = plt.subplots(figsize =(12, 8))
        for count,dat in enumerate(data):
       
            dat.plot(kind="bar", stacked=True, edgecolor='black', width=width_, 
                        ax=ax,color=colors_
                        , position=count) 

        ax.set_ylabel('Emissions GHG [Mt CO2eq]')
        ax.set_title(f'scenario comparison: \n{cases[0]}\n {cases[1]}\n {cases[2]}\n {cases[3]}\n {cases[4]}')
        ax.axhline(linewidth=1, color='black')
        ax.set_ylim(-100, 1000)
        ax.set_xlim(right=len(dat)-0.5)
        ax.legend(['line']+list(dat.columns) , loc='center left', bbox_to_anchor=(1, 0.5), ncol=1)
     
        fig.tight_layout()

        save_path = os.path.join(
            self.fig_save_path, f'multi_comparison_CO2.png')
        plt.savefig(save_path)

    def fig_storage(self):
        data = DataExtractionScenarioComparison(self.path_list).data_storage()
        print('storage')
        # print('storage',data)
        sum_data,percent_sum =[],[]  
        for t in data:
            sum_data.append(t.sum(axis=1))
            percent_sum.append(t.sum(axis=1).pct_change())

            #print('percentage change in storage',t.pct_change())
        a,b = pd.concat(sum_data,axis=1), pd.concat(percent_sum,axis=1)
        print(a.iloc[:,::-1], '\n', b.iloc[:,::-1])
        cases =[]
        for x in self.path_list:
            case= []
            cases.append(case)
            fs = os.path.dirname(x) + '/input/raw_input_data/renewables/*/*'
            for i in glob(fs):
                j = (os.path.basename(i))
                case.append(j)
        
    
        fig, ax = plt.subplots(figsize =(12, 8))
        for count,dat in enumerate(data):
       
            dat.plot(kind="bar", stacked=True, edgecolor='black', width=width_, 
                        ax=ax, position=count) 
        ax.set_ylabel(' Energy Storage [TW]')
        ax.set_title(f'scenario comparison: \n{cases[0]}\n {cases[1]}\n {cases[2]}\n {cases[3]}\n {cases[4]}')
        ax.set_ylim(0, 300)
        ax.set_xlim(right=len(dat)-0.5)
        ax.legend(dat.columns, loc='center left', bbox_to_anchor=(1, 0.5))
        axes2 = ax.twinx()
        axes2.set_ylim(0, 100)
        fig.tight_layout()

        save_path = os.path.join(
            self.fig_save_path, f'multi_comparison_storage.png')
        plt.savefig(save_path)

    def fig_imports(self):
        data = DataExtractionScenarioComparison(self.path_list).data_import_quota()
        print('imports')
        sum_data,percent_sum =[],[]  
        for t in data:
            sum_data.append(t.sum(axis=1)/1000)
            percent_sum.append(t.sum(axis=1).pct_change())

            # print('percentage change in imports ',t.pct_change())
        a,b = pd.concat(sum_data,axis=1), pd.concat(percent_sum,axis=1)
        print(a.iloc[:,::-1], '\n', b.iloc[:,::-1])
        cases =[]
        for x in self.path_list:
            case= []
            cases.append(case)
            fs = os.path.dirname(x) + '/input/raw_input_data/renewables/*/*'
            for i in glob(fs):
                j = (os.path.basename(i))
                case.append(j)

       
        fig, ax = plt.subplots(figsize =(12, 8))
        for count,dat in enumerate(data):
            
            
       
            dat.plot(kind="bar", stacked=True, edgecolor='black', width=width_, 
                        ax=ax, position=count) 

        ax.set_ylabel(' Energy share TWH')
        ax.set_title(f'scenario comparison: \n{cases[0]}\n {cases[1]}\n {cases[2]}\n {cases[3]}\n {cases[4]}')
        ax.set_ylim(0, 2800)
        ax.set_xlim(right=len(dat)-0.5)
        ax.legend(dat.columns,loc='center left', bbox_to_anchor=(1, 0.5))
        axes2 = ax.twinx()
        axes2.set_ylim(0, 100)
        fig.tight_layout()

        save_path = os.path.join(
            self.fig_save_path, f'multi_comparison_import.png')
        plt.savefig(save_path)

    def fig_electricity_generation(self):
        data_tuple = DataExtractionScenarioComparison(self.path_list).data_electricity_generation()
        data= data_tuple[0]
        line = data_tuple[1]
        print('electricity')
        #print('electricity generation',data,'\n')
        sum_data,percent_sum =[],[]  
        for t in data:
            sum_data.append(t.sum(axis=1))
            percent_sum.append(t.sum(axis=1).pct_change())

            #print('percentage change in electricity generation',t.pct_change())
        a,b = pd.concat(sum_data,axis=1), pd.concat(percent_sum,axis=1)
        print(a.iloc[:,::-1], 'cumm sum elect generation percentage \n', a.iloc[:,::-1].cumsum().pct_change(),'\n', b.iloc[:,::-1])
        cases =[]
        for x in self.path_list:
            case= []
            cases.append(case)
            fs = os.path.dirname(x) + '/input/raw_input_data/renewables/*/*'
            for i in glob(fs):
                j = (os.path.basename(i))
                case.append(j)
    
        fig, ax = plt.subplots(figsize =(12, 8))
        for count,dat in enumerate(data):
       
            dat.plot(kind="bar", stacked=True, edgecolor='black', width=width_, 
                        ax=ax,  position=count) 
        ax.set_ylabel(' Primary energy  [TWH]')#[TWH]
        ax.set_title(f'scenario comparison: \n{cases[0]}\n {cases[1]}\n {cases[2]}\n {cases[3]}\n {cases[4]}')
        ax.set_xlim(right=len(dat)-0.5)
        ax.legend(dat.columns, loc='center left', bbox_to_anchor=(1, 0.5))
        # ax.legend(dat.columns, loc='center left', bbox_to_anchor=(1, 0.5))

        # axes2 = ax.twinx()
        # axes2.plot(line.index, line.values, label='renewable share ',
        #            color='Brown')
        # axes2.set_ylim(0, 100)
        # axes2.set_ylabel("Anteil Erneuerbarer in %")
        fig.tight_layout()

        save_path = os.path.join(
            self.fig_save_path, f'multi_comparison_Electricity.png')
        plt.savefig(save_path)

         

        


    
   


   