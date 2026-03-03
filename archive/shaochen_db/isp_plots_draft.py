import sqlite3
import pandas as pd
import os
import numpy as np
import matplotlib
matplotlib.use("Agg")  # non-interactive backend - saves files without opening windows
import matplotlib.pyplot as plt
import itertools
from matplotlib.backends.backend_pdf import PdfPages
from pathlib import Path
import os

def plot_core_scenarios(
        highlight_isp,
        highlight_core,
        core_scenarios,
        df_sum,
        var_to_plot,
        title_plot,
        max_value,
):
    """Plot Generation, Utilization Factor or Capacity for the Core-ODP scenarios of 2022 and 2024 ISP

    Parameters
    ----------
    highlight_isp: ISP to highlight in the plot
    highlight_core: core scenario to highlight in the plot
    core_scenarios: dataframe with Data_source (ISP), Scenario_1 (core) and Scenario_2 (ODP). 
    df_sum: dataframe with aggregated data to plot (gen, capacity, uf)
    var_to_plot: For the figure ylabel with units. ('Generation [GWh]', 'Capacity [GW]', etc.)
    title_plot: For the figure title. ('Generation', 'Capacity', etc.)
    max_value: y-axis delimiter
    """

    scenario_colors = {
        'Step Change - Updated Inputs': 'black',
        'Step Change - Core': 'black',
        'Slow Change - Updated Inputs': 'red',
        'Progressive Change - Updated Inputs': 'blue',
        'Progressive Change - Core': 'blue',
        'Hydrogen Superpower - Updated Inputs': 'green',
        'Green Energy Exports - Core': 'green',
    }
    # Dashed for 2022, continuous for 2024 ISP
    linestyles = ['--', '-']
    isp_styles = {isp: linestyles[i % len(linestyles)] for i, isp in enumerate(core_scenarios['ISP'].unique())}
    fig, ax = plt.subplots(figsize=(12, 8)) 
    # Lineplots for each core scenario and ODP.
    for i, row in core_scenarios.iterrows():

        # Filter
        df_sum_loop = df_sum[
            (df_sum.Scenario_1 == row['core']) &
            (df_sum.Scenario_2 == row['ODP'])
        ]

        linestyle = isp_styles[row['ISP']]
        color = scenario_colors.get(row['core'], 'gray')
        
        # Highlight reference scenario with thicker line
        if row['ISP'] == highlight_isp and row['core'] == highlight_core:
            ax.plot(
                df_sum_loop.Year,
                df_sum_loop.Value,
                color='black',
                linestyle='-',
                linewidth=3.5,
                label=f"{row['ISP']} - {row['core']} (Reference scenario)"
            )
        else:
            ax.plot(
                df_sum_loop.Year,
                df_sum_loop.Value,
                linestyle=linestyle,
                color=color,
                label=f"{row['ISP']} - {row['core']}"
            )
        
    ax.set_title('Core Scenarios ODP - '+ title_plot, fontweight='bold', fontsize=16)
    ax.set_ylabel(var_to_plot, fontweight='bold', fontsize=16)
    ax.set_ylim(0, max_value)
    ax.legend(title='Core Scenarios', loc='best', fontsize=12)
    ax.grid()
    return fig
    
def plot_sensitivity_scenarios(
        core_scenarios,
        all_scenarios_odp,
        reference_scenarios,
        df_sum,
        var_to_plot,
        title_plot,
        max_value,
):
     
    """Plot Generation, Utilization Factor or Capacity for the all Sensitivity Scenarios- ODP scenarios of the 2022 and 2024 ISP

    Parameters
    ----------
    core_scenarios: dataframe with Data_source (ISP), Scenario_1 (core) and Scenario_2 (ODP). 
    all_scenarios_odp: dataframe with Data_source, Scenario_1 (all) and Scenario_2 (ODP).
    reference_scenarios: dataframe with Data_source, Scenario_1 and Scenario_2 to highlight
    df_sum: dataframe with aggregated data to plot (gen, capacity, uf)
    var_to_plot: For the figure ylabel with units. ('Generation [GWh]', 'Capacity [GW]', etc.)
    title_plot: For the figure title. ('Generation', 'Capacity', etc.)
    max_value: y-axis delimiter
    """
        
    scenario_colors = {
        'Step Change': 'black',
        'Slow Change': 'red',
        'Progressive Change': 'blue',
        'Hydrogen Superpower': 'green',
        'Green Energy Exports': 'green',
    }
    core_scenarios['scenario'] = core_scenarios['core'].str.split(' -').str[0]
    all_scenarios_odp['scenario'] = all_scenarios_odp['Scenario_1'].str.split(' -').str[0]
    for isp in core_scenarios['ISP'].unique():
        
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Filter data for the current ISP
        isp_data = all_scenarios_odp[all_scenarios_odp['Data_source'] == isp]
        
        highlight_scenario = reference_scenarios[
            reference_scenarios['ISP'] == isp]['core'].iloc[0]
        
        for i, row in isp_data.iterrows():
            # Filter capacity data
            df_sum_loop = df_sum[
                (df_sum.Data_source == isp) &
                (df_sum.Scenario_1 == row['Scenario_1']) &
                (df_sum.Scenario_2 == row['Scenario_2'])
            ]   
            color = scenario_colors.get(row['scenario'], 'gray')
            
            if (row['Scenario_1'] in core_scenarios.core.values):
                linestyle = '-'
            else:
                linestyle = '--'

            if (row['Scenario_1'] == highlight_scenario):
                ax.plot(
                    df_sum_loop.Year,
                    df_sum_loop.Value,
                    color='black',
                    linestyle='-',
                    linewidth=3.5,
                    label=f"{row['Scenario_1']} - {row['Scenario_2']}"
                )
            else:
                ax.plot(
                    df_sum_loop.Year,
                    df_sum_loop.Value,
                    linestyle=linestyle,
                    color=color,
                    label=f"{row['Scenario_1']} - {row['Scenario_2']}"
                )
        
        # Add title, labels, and legend
        ax.set_title(f'{title_plot} ODP for core and sensitivity scenarios: {isp}', fontweight='bold', fontsize=16)
        ax.set_ylabel(var_to_plot, fontweight='bold', fontsize=16)
        ax.legend(loc='best', fontsize=12)
        ax.set_ylim(0, max_value)
        ax.grid()
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close(fig)



def plot_all_cdps(
        all_scenarios_odp,
        all_scenarios,
        core_scenarios,
        odp,
        df_sum,
        var_to_plot,
        title_plot,
        max_value,
):
    
    """Plot Generation, Utilization Factor or Capacity for the all Sensitivity Scenarios- All CDP scenarios of the 2022 and 2024 ISPs

    Parameters
    ----------
    core_scenarios: dataframe with Data_source (ISP), Scenario_1 (core) and Scenario_2 (ODP). 
    all_scenarios_odp: dataframe with Data_source, Scenario_1 (all) and Scenario_2 (ODP).
    all_scenarios: dataframe with Data_source, Scenario_1 and Scenario_2 (all).
    odp: ISP - ODP info dataframe
    df_sum: dataframe with aggregated data to plot (gen, capacity, uf)
    var_to_plot: For the figure ylabel with units. ('Generation [GWh]', 'Capacity [GW]', etc.)
    title_plot: For the figure title. ('Generation', 'Capacity', etc.)
    max_value: y-axis delimiter
    """

    scenario_colors = {
        'Step Change': 'black',
        'Slow Change': 'red',
        'Progressive Change': 'blue',
        'Hydrogen Superpower': 'green',
        'Green Energy Exports': 'green',
    }
    core_scenarios['scenario'] = core_scenarios['core'].str.split(' -').str[0]
    all_scenarios['scenario'] = all_scenarios['Scenario_1'].str.split(' -').str[0]
    output_directory = output_directory = output_directory = os.path.join(os.getcwd(), 'GETRC', 'ISP Images') #NOTE: Steve edited to work locally #os.path.join('C:', os.sep, 'Users','andre','Documents','GETRC','ISP Images')
    os.makedirs(output_directory, exist_ok=True)
    output_file = os.path.join(output_directory, "all_cdps_"+ title_plot + ".pdf")
    with PdfPages(output_file) as pdf:
        for scne1 in all_scenarios_odp['Scenario_1'].unique():
            fig, ax = plt.subplots(figsize=(12, 8)) 
            
            # Filter data for the current sensitivity scenario
            isp_data = all_scenarios[all_scenarios['Scenario_1'] == scne1]
            isp = isp_data['Data_source'].iloc[0]

            highlight_scenario = odp[
                odp['Data_source'] == isp]['Scenario_2'].iloc[0]
            
            for i, row in isp_data.iterrows():
                
                df_sum_loop = df_sum[
                        (df_sum.Data_source == isp) &
                        (df_sum.Scenario_1 == row['Scenario_1']) &
                        (df_sum.Scenario_2 == row['Scenario_2'])
                ]
                color = scenario_colors.get(row['scenario'], 'gray')

                # Highlight ODP with a thicker black line
                if (row['Scenario_2'] == highlight_scenario):
                    ax.plot(
                        df_sum_loop.Year,
                        df_sum_loop.Value,
                        color='black',
                        linewidth=3.5,
                        label=f"{row['Scenario_2']}"
                    )
                # Highlight counterfactual with a dashed line
                elif (row['Scenario_2'] == 'Counterfactual'):
                    ax.plot(
                        df_sum_loop.Year,
                        df_sum_loop.Value,
                        color= color,
                        linestyle = '--',
                        label=f"{row['Scenario_2']}"
                    )
                else:
                    ax.plot(
                        df_sum_loop.Year,
                        df_sum_loop.Value,
                        color= color,
                        alpha=0.2,
                    )
    
            ax.set_title(f"{title_plot} {isp} {row['Scenario_1']}", fontweight='bold', fontsize=16)
            ax.set_ylabel(var_to_plot, fontweight='bold', fontsize=16)
            ax.legend(loc='best')
            ax.grid()
            ax.set_ylim(0, max_value)
            plt.tight_layout()

            pdf.savefig(fig)
            plt.close(fig)


def get_data(
        isp_report
):
    """ Query data from db.

    Parameter
    ----------
    isp_report: "Final" or "Draft". Specifies the type of report to query.
    """

    path = os.getcwd() #NOTE: changet to work on Steves machine # path = os.path.join('C:', os.sep, 'Users','andre','Downloads','shaochen_db','shaochen_db')
    # The path of database.
    db_path = os.path.join(path, "ISP.db")
    
    # Create database connection
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Id, Variable, Year, Value   
    capacity = pd.read_sql(
        """
            SELECT a.*, b.Data_source, b.Scenario_1, b.Scenario_2, b.State, b.Region, b.Technology
            FROM data a 
            inner join v_context_with_region b 
            on a.Id = b.Id 
            where a.variable = 'capacity' 
            and b.Data_source like :isp_report """, 
            con=conn,
            params={"isp_report": f"%{isp_report}%"},
    )
    # Clean data
    capacity = capacity[
        (capacity.Year != 'Existing and Committed') & (
            capacity.Year != 'Un33')]
    capacity = capacity[~pd.isnull(capacity.Value)]
    capacity['Value'] = capacity['Value'].astype(float)/1000
    capacity = capacity[capacity.Scenario_2 != None]
    capacity['Year'] = capacity['Year'].astype(int)
    
    generation = pd.read_sql(
        """
            SELECT a.*, b.Data_source, b.Scenario_1, b.Scenario_2, b.State, b.Region, b.Technology
            FROM data a 
            inner join v_context_with_region b 
            on a.Id = b.Id 
            where a.variable = 'generation' 
            and b.Data_source like :isp_report """, 
            con=conn,
            params={"isp_report": f"%{isp_report}%"},
    )
    # Clean data
    generation = generation[
        (generation.Year != 'Existing and Committed') & (
            generation.Year != 'Un33')]
    generation = generation[~pd.isnull(generation.Value)]
    generation = generation[generation.Scenario_2 != None]
    generation['Value'] = generation['Value'].astype(float)
    generation['Year'] = generation['Year'].astype(int)
#    storage_energy = pd.read_sql(
#        """
#            SELECT a.*, b.Data_source, b.Scenario_1, b.Scenario_2, b.State, b.Region, b.Technology
#            FROM data a 
#            inner join v_context_with_region b 
#            on a.Id = b.Id 
#            where a.variable = 'storage energy' 
#            and b.Data_source like :isp_report """, 
#            con=conn,
#            params={"isp_report": f"%{isp_report}%"},
#    )
#    storage_capacity = pd.read_sql(
#        """
#            SELECT a.*, b.Data_source, b.Scenario_1, b.Scenario_2, b.State, b.Region, b.Technology
#            FROM data a 
#            inner join v_context_with_region b 
#            on a.Id = b.Id 
#            where a.variable = 'storage capacity' 
#            and b.Data_source like :isp_report """, 
#            con=conn,
#            params={"isp_report": f"%{isp_report}%"},
#    )
    
    # Data_source, Attribute_type, Original_value, Standard_value
    # Original_value = Standard_value
#    mapping = pd.read_sql(
#        "SELECT * FROM mapping", con=conn
#    )
    # Columns
    # Id, Data_source, Scenario_1, Scenario_2, State, Region, Technology
    context = pd.read_sql(
        """
            SELECT * 
            FROM v_context_with_region 
            where Data_source like :isp_report """, 
            con=conn,
            params={"isp_report": f"%{isp_report}%"},
    )
    
    conn.commit()
    conn.close()

    return capacity, generation, context

def plot_stack_by_reg(
        df,
        scenarios,
        max_value,
        filename,
        grp_by,
        agg_by,
        stack_order,
        var_to_plot,
):
    
    """Plot Generation, Utilization Factor or Capacity for the all Sensitivity Scenarios- All CDP scenarios of the 2022 and 2024 ISPs

    Parameters
    ----------
    df: dataframe with data to plot (gen, capacity, uf)
    scenarios: dataframe with scenarios to plot (core, sensitivity, all, etc.)
    max_value: y-axis delimiter
    filename: name of the pdf file with the plots
    grp_by: list of columns to group by
    agg_by: level of granularity of the data (Region, State)
    stack_order: Order to plot the stack
    var_to_plot: For the figure ylabel with units. ('Generation [GWh]', 'Capacity [GW]', etc.)
    """
        
    if agg_by == 'Region':
        region_name = pd.DataFrame([
            {"Region": "NQ", "Name": "Northern Queensland"},                # QLD
            {"Region": "GG", "Name": "Gladstone"},                          # QLD
            {"Region": "CQ", "Name": "Central Queensland"},                 # QLD
            {"Region": "SQ", "Name": "Southern Queensland"},                # QLD
            {"Region": "NNSW", "Name": "Northern New South Wales"},         # NSW
            {"Region": "CNSW", "Name": "Central New South Wales"},          # NSW
            {"Region": "SNSW", "Name": "Southern New South Wales"},         # NSW
            {"Region": "SNW", "Name": "Sydney, Newcastle & Wollongong"},    # NSW
            {"Region": "VIC", "Name": "Victoria"},                          # VIC
            {"Region": "SESA", "Name": "South East South Australia"},       # SA
            {"Region": "CSA", "Name": "Central South Australia"},           # SA
            {"Region": "TAS", "Name": "Tasmania"},                          # TAS
        ])
        color_mapping = {
            "Northern Queensland": "#A9A9A9",                               # Light Gray
            "Gladstone": "#808080",                                         # Medium Gray
            "Central Queensland": "#696969",                                # Dark Gray
            "Southern Queensland": "#505050",                               # Deeper Gray
            "Northern New South Wales": "#FF9999",                          # Light Red
            "Central New South Wales": "#FF6666",                           # Medium Red
            "Southern New South Wales": "#FF3333",                          # Dark Red
            "Sydney, Newcastle & Wollongong": "#CC0000",                    # Deep Red
            "Victoria": "#228B22",                                          # Green
            "South East South Australia": "#FFD54F",                        # Light Yellow
            "Central South Australia": "#FFA000",                           # Medium Orange
            "Tasmania": "#4169E1",                                          # Blue
        }

    else:
        region_name = pd.DataFrame([
            {"State": "QLD", "Name": "QLD"},
            {"State": "NSW", "Name": "NSW"},
            {"State": "VIC", "Name": "VIC"},
            {"State": "SA", "Name": "SA"},
            {"State": "TAS", "Name": "TAS"},
        ])
        color_mapping = {
            "QLD": "#808080",                                               # Gray for Queensland
            "NSW": "#FF0000",                                               # Red for NSW
            "VIC": "#008000",                                               # Green for Victoria
            "SA": "#FFB300",                                                # Orange for South Australia
            "TAS": "#0000FF",                                               # Blue for Tasmania
        }
        
    # Group by granularity level
    df_by_reg = df.groupby(
        grp_by, as_index = False
    )['Value'].sum()
    # Filter for wanted scenarios (sensitivity and cdp)
    df_by_reg = df_by_reg.merge(
        scenarios,
        how = 'inner',
        )
    # Filter out regions/states with negligible contributions
    ##########
    df_by_reg = df_by_reg[
        df_by_reg.Value > 0.1]

    df_by_reg = df_by_reg.merge(
        region_name,
        how = 'left',
        on = agg_by,
    )
    
    grouped = df_by_reg.groupby(['Data_source', 'Scenario_1', 'Scenario_2'])
    output_directory = output_directory = os.path.join(os.getcwd(), 'GETRC', 'ISP Images') #NOTE: Changed bu Steve to work locally #os.path.join('C:', os.sep, 'Users','andre','Documents','GETRC','ISP Images')
    os.makedirs(output_directory, exist_ok=True)
    output_file = os.path.join(output_directory, filename)
    with PdfPages(output_file) as pdf:
    
        for (Data_source, scenario_1, scenario_2), group in grouped:
            
            fig, ax = plt.subplots(figsize=(12, 8))
            pivot_data = group.pivot(index='Year', columns='Name', values='Value').fillna(0)
            pivot_data = pivot_data.reindex(columns=stack_order, fill_value=0)


            colors = [color_mapping[name] for name in stack_order if name in color_mapping]
            
            if var_to_plot == 'Capacity':
                pivot_data.plot(kind='area', stacked=True, alpha=0.8, ax=ax, color=colors)
                ylabel = 'Capacity [GW]'
                title_in_plot = f'Stacked Area Plot: {Data_source} - {scenario_1} - {scenario_2}'
            elif var_to_plot == 'UF':
                pivot_data.plot(kind='line', alpha=0.8, ax=ax, color=colors)
                ylabel = 'UF [GWh / GWh] x 100'
                title_in_plot = f'{Data_source} - {scenario_1} - {scenario_2}'
            else:
                print('Error')
            ax.set_title(title_in_plot, fontweight='bold', fontsize=16)
            ax.set_ylabel(ylabel, fontweight='bold', fontsize=16)
            ax.tick_params(axis='x', labelsize=14)
            ax.tick_params(axis='y', labelsize=14)
            ax.set_ylim(0, max_value)
            ax.set_xlabel("")
            ax.grid(True, linestyle='--', alpha=0.5)

            handles, labels = ax.get_legend_handles_labels()
            plotted_regions = pivot_data.columns[pivot_data.sum(axis=0) > 0]
            valid_handles = [handles[labels.index(lbl)] for lbl in plotted_regions if lbl in labels]
            valid_labels = [lbl for lbl in plotted_regions if lbl in labels]
            

            ax.legend(
                valid_handles[::-1],  # Reverse order
                valid_labels[::-1],  # Reverse order
                title=agg_by,
                loc='best'
            )
            
            plt.tight_layout()
                
            # Save the current figure to a new page in the PDF
            pdf.savefig(fig)
            
            # Close the figure
            plt.close(fig)


def plot_stack_by_reg_perc(
        df,
        scenarios,
        max_value,
        filename,
        grp_by,
        agg_by,
        stack_order,
        var_to_plot,
):
    
    """Plot Generation, Utilization Factor or Capacity for the all Sensitivity Scenarios- All CDP scenarios of the 2022 and 2024 ISPs

    Parameters
    ----------
    df: dataframe with data to plot (gen, capacity, uf)
    scenarios: dataframe with scenarios to plot (core, sensitivity, all, etc.)
    max_value: y-axis delimiter
    filename: name of the pdf file with the plots
    grp_by: list of columns to group by
    agg_by: level of granularity of the data (Region, State)
    stack_order: Order to plot the stack
    var_to_plot: For the figure ylabel with units. ('Generation [GWh]', 'Capacity [GW]', etc.)
    """
        
    if agg_by == 'Region':
        region_name = pd.DataFrame([
            {"Region": "NQ", "Name": "Northern Queensland"},                # QLD
            {"Region": "GG", "Name": "Gladstone"},                          # QLD
            {"Region": "CQ", "Name": "Central Queensland"},                 # QLD
            {"Region": "SQ", "Name": "Southern Queensland"},                # QLD
            {"Region": "NNSW", "Name": "Northern New South Wales"},         # NSW
            {"Region": "CNSW", "Name": "Central New South Wales"},          # NSW
            {"Region": "SNSW", "Name": "Southern New South Wales"},         # NSW
            {"Region": "SNW", "Name": "Sydney, Newcastle & Wollongong"},    # NSW
            {"Region": "VIC", "Name": "Victoria"},                          # VIC
            {"Region": "SESA", "Name": "South East South Australia"},       # SA
            {"Region": "CSA", "Name": "Central South Australia"},           # SA
            {"Region": "TAS", "Name": "Tasmania"},                          # TAS
        ])
        color_mapping = {
            "Northern Queensland": "#A9A9A9",                               # Light Gray
            "Gladstone": "#808080",                                         # Medium Gray
            "Central Queensland": "#696969",                                # Dark Gray
            "Southern Queensland": "#505050",                               # Deeper Gray
            "Northern New South Wales": "#FF9999",                          # Light Red
            "Central New South Wales": "#FF6666",                           # Medium Red
            "Southern New South Wales": "#FF3333",                          # Dark Red
            "Sydney, Newcastle & Wollongong": "#CC0000",                    # Deep Red
            "Victoria": "#228B22",                                          # Green
            "South East South Australia": "#FFD54F",                        # Light Yellow
            "Central South Australia": "#FFA000",                           # Medium Orange
            "Tasmania": "#4169E1",                                          # Blue
        }

    else:
        region_name = pd.DataFrame([
            {"State": "QLD", "Name": "QLD"},
            {"State": "NSW", "Name": "NSW"},
            {"State": "VIC", "Name": "VIC"},
            {"State": "SA", "Name": "SA"},
            {"State": "TAS", "Name": "TAS"},
        ])
        color_mapping = {
            "QLD": "#808080",                                               # Gray for Queensland
            "NSW": "#FF0000",                                               # Red for NSW
            "VIC": "#008000",                                               # Green for Victoria
            "SA": "#FFB300",                                                # Orange for South Australia
            "TAS": "#0000FF",                                               # Blue for Tasmania
        }
        
    # Filter out regions/states with negligible contributions
    ##########
    df_by_reg = df[
        df.Value > 0.1]

    df_by_reg = df_by_reg.merge(
        region_name,
        how = 'left',
        on = agg_by,
    )
    
    grouped = df_by_reg.groupby(['Data_source', 'Scenario_1', 'Scenario_2'])
    output_directory = output_directory = os.path.join(os.getcwd(), 'GETRC', 'ISP Images') #NOTE: Changed bu Steve to work locally #os.path.join('C:', os.sep, 'Users','andre','Documents','GETRC','ISP Images')
    os.makedirs(output_directory, exist_ok=True)
    output_file = os.path.join(output_directory, filename)
    with PdfPages(output_file) as pdf:
    
        for (Data_source, scenario_1, scenario_2), group in grouped:
            
            fig, ax = plt.subplots(figsize=(12, 8))
            pivot_data = group.pivot(index='Year', columns='Name', values='Value').fillna(0)
            pivot_data = pivot_data.reindex(columns=stack_order, fill_value=0)


            colors = [color_mapping[name] for name in stack_order if name in color_mapping]
            
            if var_to_plot == 'Capacity':
                pivot_data.plot(kind='area', stacked=True, alpha=0.8, ax=ax, color=colors)
                ylabel = 'Capacity [GW]'
                title_in_plot = f'Stacked Area Plot: {Data_source} - {scenario_1} - {scenario_2}'
            elif var_to_plot == 'UF':
                pivot_data.plot(kind='line', alpha=0.8, ax=ax, color=colors)
                ylabel = 'UF [GWh / GWh] x 100'
                title_in_plot = f'{Data_source} - {scenario_1} - {scenario_2}'
            elif var_to_plot == 'GPG_per':
                pivot_data.plot(kind='area', stacked=True, alpha=0.8, ax=ax, color=colors)
                ylabel = 'GPG Percentage [GWh / GWh] x 100'
                title_in_plot = f'{Data_source} - {scenario_1} - {scenario_2}'
            else:
                print('Error')
            ax.set_title(title_in_plot, fontweight='bold', fontsize=16)
            ax.set_ylabel(ylabel, fontweight='bold', fontsize=16)
            ax.tick_params(axis='x', labelsize=14)
            ax.tick_params(axis='y', labelsize=14)
            ax.set_ylim(0, max_value)
            ax.set_xlabel("")
            ax.grid(True, linestyle='--', alpha=0.5)

            handles, labels = ax.get_legend_handles_labels()
            plotted_regions = pivot_data.columns[pivot_data.sum(axis=0) > 0]
            valid_handles = [handles[labels.index(lbl)] for lbl in plotted_regions if lbl in labels]
            valid_labels = [lbl for lbl in plotted_regions if lbl in labels]
            

            ax.legend(
                valid_handles[::-1],  # Reverse order
                valid_labels[::-1],  # Reverse order
                title=agg_by,
                loc='best'
            )
            
            plt.tight_layout()
                
            # Save the current figure to a new page in the PDF
            pdf.savefig(fig)
            
            # Close the figure
            plt.close(fig)

    

isp_report = 'Final'

#isp_report = 'Draft'
## Change the scenario values since they are different for draft version
(
 capacity,
 generation, 
 # storage_energy, 
 # storage_capacity, 
 # mapping, 
 context,
) = get_data(
    isp_report
)

all_scenarios = context[['Data_source','Scenario_1','Scenario_2']].drop_duplicates().dropna()
core_scenarios = pd.DataFrame([
    {"ISP": "2022 " + isp_report+ " ISP", "core": "Hydrogen Superpower - Updated Inputs", "ODP": "CDP12"},
    {"ISP": "2022 " + isp_report+ " ISP", "core": "Progressive Change - Updated Inputs", "ODP": "CDP12"},
    {"ISP": "2022 " + isp_report+ " ISP", "core": "Slow Change - Updated Inputs", "ODP": "CDP12"},
    {"ISP": "2022 " + isp_report+ " ISP", "core": "Step Change - Updated Inputs", "ODP": "CDP12"},
    {"ISP": "2024 " + isp_report+ " ISP", "core": "Step Change - Core", "ODP": "CDP14"},
    {"ISP": "2024 " + isp_report+ " ISP", "core": "Progressive Change - Core", "ODP": "CDP14"},
    {"ISP": "2024 " + isp_report+ " ISP", "core": "Green Energy Exports - Core", "ODP": "CDP14"}
])
odp = pd.DataFrame({
    "Data_source": [
            "2022 " + isp_report+ " ISP",
            "2024 " + isp_report+ " ISP"
        ],
    "Scenario_2": [
            "CDP12",
            "CDP14"
        ]
})
# Reference scenarios will be highlighted in plots with thick black lines.
reference_scenarios = pd.DataFrame({
    "ISP": ["2022 " + isp_report+ " ISP", 
            "2024 " + isp_report+ " ISP"
            ],
    "core": ['Step Change - Updated Inputs',
             'Step Change - Core'
             ]
})
all_scenarios_odp = all_scenarios.merge(odp, how = 'inner')

# Generation technologies considered GPG. To be plotted.
gpg_tech = [
    'Mid-merit Gas',
    'Mid-merit Gas with CCS',
    'Peaking Gas+Liquids',
    'Flexible gas',
    'Flexible gas with CCS',
    'Mid-merit gas',
]

coal_tech = [
    'Black Coal',
    'Black coal',
    'Brown Coal',
    'Brown coal',
]
# Filter capacity data.
capacity_gpg = capacity[
    (capacity.Technology.isin(coal_tech))
]
generation_gpg = generation[
    (generation.Technology.isin(coal_tech))
]

# Max anunual generation = Installed Capacity X Hours in a day X Days in a year
capacity_gpg['max_annual_gen'] = capacity_gpg['Value']*24*365

# Installed capacity per year for each sceneario and CDP.
capacity_gpg_sum = capacity_gpg.groupby(
    ['Data_source','Scenario_1','Scenario_2','Year'], as_index = False
).agg({'Value':'sum','max_annual_gen':'sum'})
# Installed capacity per year for each sceneario, CDP and state.
capacity_gpg_sum_bystate = capacity_gpg.groupby(
    ['Data_source','Scenario_1','Scenario_2','State','Year'], as_index = False
).agg({'Value':'sum','max_annual_gen':'sum'})
# Installed capacity per year for each sceneario, CDP, state and sub-region (only 2024 has sub-region granularity).
capacity_gpg_sum_byregion = capacity_gpg.groupby(
    ['Data_source','Scenario_1','Scenario_2','State','Region','Year'], as_index = False
).agg({'Value':'sum','max_annual_gen':'sum'})

# Forecasted generation per year for each sceneario and CDP.
generation_gpg_sum = generation_gpg.groupby(
    ['Data_source','Scenario_1','Scenario_2','Year'], as_index=False
)['Value'].sum()
# Forecasted generation per year for each sceneario, CDP and state.
generation_gpg_sum_bystate = generation_gpg.groupby(
    ['Data_source','Scenario_1','Scenario_2','State','Year'], as_index=False
)['Value'].sum()
# Forecasted generation per year for each sceneario, CDP, state and sub-region (only 2024 has sub-region granularity).
generation_gpg_sum_byregion = generation_gpg.groupby(
    ['Data_source','Scenario_1','Scenario_2','State','Region','Year'], as_index=False
)['Value'].sum()


# Forecasted TOTAL generation per year for each sceneario and CDP.

generation_pos = generation[generation.Value>0]
generation_sum = generation_pos.groupby(
    ['Data_source','Scenario_1','Scenario_2','Year'], as_index=False
)['Value'].sum()
# Forecasted generation per year for each sceneario, CDP and state.
generation_sum_bystate = generation_pos.groupby(
    ['Data_source','Scenario_1','Scenario_2','State','Year'], as_index=False
)['Value'].sum()
# Forecasted generation per year for each sceneario, CDP, state and sub-region (only 2024 has sub-region granularity).
generation_sum_byregion = generation_pos.groupby(
    ['Data_source','Scenario_1','Scenario_2','State','Region','Year'], as_index=False
)['Value'].sum()


generation_sum = generation_sum.merge(
    generation_gpg_sum.rename(columns={'Value':'GPG'}),
    how = 'inner',
    on = ['Data_source','Scenario_1','Scenario_2','Year'],
)
generation_sum['Value'] = generation_sum['GPG']/generation_sum['Value']*100
generation_sum_bystate = generation_sum_bystate.merge(
    generation_gpg_sum_bystate.rename(columns={'Value':'GPG'}),
    how = 'inner',
    on = ['Data_source','Scenario_1','Scenario_2','Year','State'],
)
generation_sum_bystate['Value'] = generation_sum_bystate['GPG']/generation_sum_bystate['Value']*100
generation_sum_byregion = generation_sum_byregion.merge(
    generation_gpg_sum_byregion.rename(columns={'Value':'GPG'}),
    how = 'inner',
    on = ['Data_source','Scenario_1','Scenario_2','Year','State','Region'],
)
generation_sum_byregion['Value'] = generation_sum_byregion['GPG']/generation_sum_byregion['Value']*100



# Utilization factor is ratio between Generation[GWh] / MAX_generation [GWh]
util_factor_gpg_bystate = capacity_gpg_sum_bystate[['Data_source','Scenario_1','Scenario_2','State','Year','max_annual_gen']].merge(
    generation_gpg_sum_bystate, 
    how = 'left',
    on = ['Data_source','Scenario_1','Scenario_2','State','Year']
    )
util_factor_gpg_byregion = capacity_gpg_sum_byregion[['Data_source','Scenario_1','Scenario_2','State','Region','Year','max_annual_gen']].merge(
    generation_gpg_sum_byregion, 
    how = 'left',
    on = ['Data_source','Scenario_1','Scenario_2','State','Region','Year']
    )
util_factor_gpg = capacity_gpg_sum[['Data_source','Scenario_1','Scenario_2','Year','max_annual_gen']].merge(
    generation_gpg_sum,
    how = 'inner',
    on = ['Data_source','Scenario_1','Scenario_2','Year']
    )
util_factor_gpg_bystate['Value'] = util_factor_gpg_bystate['Value']/util_factor_gpg_bystate['max_annual_gen']*100
util_factor_gpg_byregion['Value'] = util_factor_gpg_byregion['Value']/util_factor_gpg_byregion['max_annual_gen']*100
util_factor_gpg['Value'] = util_factor_gpg['Value']/util_factor_gpg['max_annual_gen']*100


# Plot to be highlighted (thick black line)
highlight_isp = '2024 '+ isp_report + ' ISP'
highlight_core = 'Step Change - Core'

max_value = 26
# Plot 1: Core scenarios 

with PdfPages('Coal_Cap_UF_Core_ODP_and_ODP_all_sensitivity.pdf') as pdf:
    fig = plot_core_scenarios(
            highlight_isp,
            highlight_core,
            core_scenarios,
            capacity_gpg_sum,
            'Capacity [GW]',
            'Capacity',
            max_value,
    )
    pdf.savefig(fig)
    plt.close(fig)
    plot_sensitivity_scenarios(
            core_scenarios,
            all_scenarios_odp,
            reference_scenarios,
            capacity_gpg_sum,
            'Capacity [GW]',
            'Capacity',
            max_value,
    )

    fig = plot_core_scenarios(
            highlight_isp,
            highlight_core,
            core_scenarios,
            util_factor_gpg,
            'UF [GWh / GWh x 100]',
            'UF',
            100,
    )
    pdf.savefig(fig)
    plt.close(fig)
    # Plot 2
    
    plot_sensitivity_scenarios(
            core_scenarios,
            all_scenarios_odp,
            reference_scenarios,
            util_factor_gpg,
            'UF [GWh / GWh x 100]',
            'UF',
            100,
    )

    fig = plot_core_scenarios(
        highlight_isp,
        highlight_core,
        core_scenarios,
        generation_gpg_sum,
        'Generation [GWh]',
        'Generation',
        140000,
    )
    pdf.savefig(fig)
    plt.close(fig)
    plot_sensitivity_scenarios(
        core_scenarios,
        all_scenarios_odp,
        reference_scenarios,
        generation_gpg_sum,
        'Generation [GWh]',
        'Generation',
        140000,
    )


# with PdfPages('Coal_Percentage_of_TotalGen_Core_ODP_and_ODP_all_sensitivity.pdf') as pdf:
#     fig = plot_core_scenarios(
#             highlight_isp,
#             highlight_core,
#             core_scenarios,
#             generation_sum,
#             'GPG Gen / Total Gen [%]',
#             'GPG Generation Percentage',
#             35,
#     )
#     pdf.savefig(fig)
#     plt.close(fig)
#     plot_sensitivity_scenarios(
#             core_scenarios,
#             all_scenarios_odp,
#             reference_scenarios,
#             generation_sum,
#             'GPG Gen / Total Gen [%]',
#             'GPG Generation Percentage',
#             35,
#     )


# plot_all_cdps(
#         all_scenarios_odp,
#         all_scenarios,
#         core_scenarios,
#         odp,
#         generation_sum,
#         'GPG Gen / Total Gen [%]',
#         'GPG Generation Percentage',
#         35,
# )

# Plot 3
plot_all_cdps(
        all_scenarios_odp,
        all_scenarios,
        core_scenarios,
        odp,
        capacity_gpg_sum,
        'Capacity [GW]',
        'Capacity - Coal',
        max_value,
)


plot_all_cdps(
        all_scenarios_odp,
        all_scenarios,
        core_scenarios,
        odp,
        util_factor_gpg,
        'UF [GWh / GWh x 100]',
        'UF - Coal',
        100,
)

plot_all_cdps(
        all_scenarios_odp,
        all_scenarios,
        core_scenarios,
        odp,
        generation_gpg_sum,
        'Generation [GWh]',
        'Generation - Coal',
        140000,
)
            


grp_by_state = ['Data_source','Scenario_1','Scenario_2','Year','State']
grp_by_reg = grp_by_state + ['Region']

stack_order_reg = [
    "Northern Queensland",
    "Gladstone",
    "Central Queensland",
    "Southern Queensland",
    "Northern New South Wales",
    "Central New South Wales",
    "Southern New South Wales",
    "Sydney, Newcastle & Wollongong",
    "Victoria",
    "South East South Australia",
    "Central South Australia",
    "Tasmania",
]

plot_stack_by_reg(
        capacity_gpg,
        core_scenarios.rename(columns={'ISP':'Data_source','core':'Scenario_1','ODP':'Scenario_2',}),
        max_value,
        "2024_Coal_core_odp_capacity.pdf",
        grp_by_reg,
        "Region",
        stack_order_reg,
        'Capacity'
)
plot_stack_by_reg(
        capacity_gpg,
        all_scenarios_odp,
        max_value,
        "2024_Coal_all_sc_odp_capacity.pdf",
        grp_by_reg,
        "Region",
        stack_order_reg,
        'Capacity',
)
# UF 

# util_factor_gpg_bystate
# util_factor_gpg_byregion
plot_stack_by_reg(
        util_factor_gpg_byregion,
        core_scenarios.rename(columns={'ISP':'Data_source','core':'Scenario_1','ODP':'Scenario_2',}),
        100,
        "2024_Coal_core_odp_UF_byregion.pdf",
        grp_by_reg,
        "Region",
        stack_order_reg,
        'UF'
)
plot_stack_by_reg(
        util_factor_gpg_byregion,
        all_scenarios_odp,
        100,
        "2024_Coal_all_scenarios_odp_UF.pdf",
        grp_by_reg,
        "Region",
        stack_order_reg,
        'UF'
)
# GPG Percentage
# plot_stack_by_reg_perc(
#         generation_sum_byregion,
#         core_scenarios.rename(columns={'ISP':'Data_source','core':'Scenario_1','ODP':'Scenario_2',}),
#         100,
#         "Stacked_2024_core_odp_GPG_percentage.pdf",
#         grp_by_reg,
#         "Region",
#         stack_order_reg,
#         'GPG_per'
# )
# plot_stack_by_reg_perc(
#         generation_sum_byregion,
#         all_scenarios_odp,
#         100,
#         "Stacked_2024_all_sc_odp_GPG_percentage.pdf",
#         grp_by_reg,
#         "Region",
#         stack_order_reg,
#         'GPG_per'
# )

generation_sum_byregion_filt = generation_sum_byregion[generation_sum_byregion.Scenario_1 == 'Green Energy Exports - Core']
generation_sum_byregion_filt = generation_sum_byregion_filt[generation_sum_byregion_filt.Scenario_2 == 'CDP1']
generation_sum_byregion_filt = generation_sum_byregion_filt[generation_sum_byregion_filt.Year == 2025]


# 2022
stack_order_state = ['QLD', 'NSW', 'VIC','SA', 'TAS']
capacity_gpg_2022 = capacity_gpg[
    capacity_gpg.Data_source == '2022 Final ISP']
util_factor_gpg_bystate_2022 = util_factor_gpg_bystate[
    util_factor_gpg_bystate.Data_source == '2022 Final ISP']
plot_stack_by_reg(
        capacity_gpg_2022,
        core_scenarios.rename(columns={'ISP':'Data_source','core':'Scenario_1','ODP':'Scenario_2',}),
        max_value,
        "2022_Coal_core_odp_capacity.pdf",
        grp_by_state,
        "State",
        stack_order_state,
        'Capacity'
)

plot_stack_by_reg(
        capacity_gpg_2022,
        all_scenarios_odp,
        max_value,
        "2022_Coal_all_sc_odp_capacity.pdf",
        grp_by_state,
        "State",
        stack_order_state,
        'Capacity'
)
    
plot_stack_by_reg(
        util_factor_gpg_bystate_2022,
        core_scenarios.rename(columns={'ISP':'Data_source','core':'Scenario_1','ODP':'Scenario_2',}),
        100,
        "2022_Coal_core_odp_UF_bystate.pdf",
        grp_by_state,
        "State",
        stack_order_state,
        'UF'
)

plot_stack_by_reg(
        util_factor_gpg_bystate_2022,
        all_scenarios_odp,
        100,
        "2022_Coal_all_scenarios_odp_UF_bystate.pdf",
        grp_by_state,
        "State",
        stack_order_state,
        'UF'
)
    
# For the stack, try this order initially (bottom to top) – Qld, NSW, Vic, SA, Tas
# For UF, the equivalent would be line plots
# Do this for:
# each of the ISP2024 & ISP2022 ‘core’ scenarios (ODP)
# pick a few extreme outliers in terms of total NEM GPG, and see whether (or not) the sub-regional split varies much

core_all_cdp = core_scenarios[['ISP','core']].merge(
    all_scenarios,
    how = 'left',
    left_on = ['ISP','core'],
    right_on = ['Data_source','Scenario_1'],
    )
# Then repeat for each of the following:
# SQ
# Sydney
# Victoria
# Central South Australia
sub_regions = ['SQ','VIC','SNW','CSA']

for reg in sub_regions:
    
    capacity_gpg_filt = capacity_gpg_sum_byregion[
        capacity_gpg_sum_byregion.Region == reg]

    core_scenarios_filt = core_scenarios[
        core_scenarios.ISP == '2024 Final ISP']
    all_scenarios_odp_filt = all_scenarios_odp[
        all_scenarios_odp.Data_source == '2024 Final ISP']
    all_scenarios_filt = all_scenarios[
        all_scenarios.Data_source == '2024 Final ISP']
    
    util_factor_gpg_byregion_filt = util_factor_gpg_byregion[
        util_factor_gpg_byregion.Region == reg]

    
    plot_all_cdps(
            all_scenarios_odp_filt,
            all_scenarios_filt,
            core_scenarios_filt,
            odp,
            capacity_gpg_filt,
            'Capacity [GW]',
            f'{reg} - Capacity - Coal',
            6,
    )

    plot_all_cdps(
            all_scenarios_odp_filt,
            all_scenarios_filt,
            core_scenarios_filt,
            odp,
            util_factor_gpg_byregion_filt,
            'UF [GWh / GWh x 100]',
            f'{reg} - UF - Coal',
            100,
    )





for reg in sub_regions:
    
    generation_gpg_filt = generation_gpg_sum_byregion[
        generation_gpg_sum_byregion.Region == reg]

    core_scenarios_filt = core_scenarios[
        core_scenarios.ISP == '2024 Final ISP']
    all_scenarios_odp_filt = all_scenarios_odp[
        all_scenarios_odp.Data_source == '2024 Final ISP']
    all_scenarios_filt = all_scenarios[
        all_scenarios.Data_source == '2024 Final ISP']

    plot_all_cdps(
            all_scenarios_odp_filt,
            all_scenarios_filt,
            core_scenarios_filt,
            odp,
            generation_gpg_filt,
            'Generation [GWh]',
            f'{reg} - Generation - Coal',
            140000,
    )