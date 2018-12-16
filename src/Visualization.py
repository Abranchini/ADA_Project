import math
import operator
import numpy as np
import pandas as pd

import plotly
import plotly.graph_objs as go
import plotly.plotly as py

from plotly.tools import FigureFactory as FF
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
from IPython.display import display, HTML
from plotly.grid_objs import Grid, Column


plotly.tools.set_credentials_file(username='StudentUni', api_key='8EBJTXg2lf04Yxb03QEw')

def visualization_q2(data_q2_final):

    data_q2_final = data_q2_final.sort_values(['continent', 'country'])
    data_q2_final['pop_num'] = data_q2_final.iloc[:,4].str.replace(',', '').astype(int)
    slope = 2.666051223553066e-05
    hover_text = []
    bubble_size = []

    for index, row in data_q2_final.iterrows():
        hover_text.append(('Country: {country}<br>'+
                          'Number of events: {numev}<br>'+
                          'Number of articles: {numart}<br>'+
                          'Population: {pop}<br>').format(country=row['country'],
                                                numev=row['count_events'],
                                                numart=row['sum_articles'],
                                                pop=row['pop']))
        bubble_size.append(math.sqrt(row['pop_num']*slope))

    data_q2_final['text'] = hover_text
    data_q2_final['size'] = bubble_size
    sizeref = 2.*max(data_q2_final['size'])/(100**2)

    trace0 = go.Scatter(
        x=data_q2_final['count_events'][data_q2_final['continent'] == 'Africa'],
        y=data_q2_final['sum_articles'][data_q2_final['continent'] == 'Africa'],
        mode='markers',
        name='Africa',
        text=data_q2_final['text'][data_q2_final['continent'] == 'Africa'],
        marker=dict(
            symbol='circle',
            sizemode='area',
            sizeref=sizeref,
            size=data_q2_final['size'][data_q2_final['continent'] == 'Africa'],
            line=dict(
                width=2
            ),
        )
    )
    trace1 = go.Scatter(
        x=data_q2_final['count_events'][data_q2_final['continent'] == 'Americas'],
        y=data_q2_final['sum_articles'][data_q2_final['continent'] == 'Americas'],
        mode='markers',
        name='Americas',
        text=data_q2_final['text'][data_q2_final['continent'] == 'Americas'],
        marker=dict(
            sizemode='area',
            sizeref=sizeref,
            size=data_q2_final['size'][data_q2_final['continent'] == 'Americas'],
            line=dict(
                width=2
            ),
        )
    )
    trace2 = go.Scatter(
        x=data_q2_final['count_events'][data_q2_final['continent'] == 'Asia'],
        y=data_q2_final['sum_articles'][data_q2_final['continent'] == 'Asia'],
        mode='markers',
        name='Asia',
        text=data_q2_final['text'][data_q2_final['continent'] == 'Asia'],
        marker=dict(
            sizemode='area',
            sizeref=sizeref,
            size=data_q2_final['size'][data_q2_final['continent'] == 'Asia'],
            line=dict(
                width=2
            ),
        )
    )
    trace3 = go.Scatter(
        x=data_q2_final['count_events'][data_q2_final['continent'] == 'Europe'],
        y=data_q2_final['sum_articles'][data_q2_final['continent'] == 'Europe'],
        mode='markers',
        name='Europe',
        text=data_q2_final['text'][data_q2_final['continent'] == 'Europe'],
        marker=dict(
            sizemode='area',
            sizeref=sizeref,
            size=data_q2_final['size'][data_q2_final['continent'] == 'Europe'],
            line=dict(
                width=2
            ),
        )
    )
    trace4 = go.Scatter(
        x=data_q2_final['count_events'][data_q2_final['continent'] == 'Oceania'],
        y=data_q2_final['sum_articles'][data_q2_final['continent'] == 'Oceania'],
        mode='markers',
        name='Oceania',
        text=data_q2_final['text'][data_q2_final['continent'] == 'Oceania'],
        marker=dict(
            sizemode='area',
            sizeref=sizeref,
            size=data_q2_final['size'][data_q2_final['continent'] == 'Oceania'],
            line=dict(
                width=2
            ),
        )
    )

    data = [trace0, trace1, trace2, trace3, trace4]
    layout = go.Layout(
        title='Number of Articles vs Number of Events',
        xaxis=dict(
            title='Number of Events',
            gridcolor='rgb(255, 255, 255)',
            range=[0, 7.5e07],
            zerolinewidth=1,
            ticklen=5,
            gridwidth=2,
        ),
        yaxis=dict(
            title='Number of Articles',
            gridcolor='rgb(255, 255, 255)',
            range=[0, 3.5e08],
            zerolinewidth=1,
            ticklen=5,
            gridwidth=2,
        ),
        paper_bgcolor='rgb(243, 243, 243)',
        plot_bgcolor='rgb(243, 243, 243)',
    )

    fig = go.Figure(data=data, layout=layout)
    iplot(fig)
    return

def visualization_Q4(dataframe):
    """ Make visualization for question 4
        Input:
            dataframe
        Output:
            plotly image
    """

    df_0 = dataframe
    years_from_col = set(df_0['year'])
    years_ints = sorted(list(years_from_col))
    years = [str(year) for year in years_ints]

    # make list of continents
    continents = []
    for continent in df_0['region']:
        if continent not in continents: 
            continents.append(continent)

    columns = []
    # make grid
    for year in years:
        for continent in continents:
            dataset_by_year = df_0[df_0['year'] == year]
            dataset_by_year_and_cont = dataset_by_year[dataset_by_year['region'] == continent]
            
            for col_name in dataset_by_year_and_cont:
                # each column name is unique
                column_name = '{year}_{continent}_{header}_gapminder_grid'.format(
                    year=year, continent=continent, header=col_name
                )
                a_column = Column(list(dataset_by_year_and_cont[col_name]), column_name)
                columns.append(a_column)

    # upload grid
    grid = Grid(columns)
    url = py.grid_ops.upload(grid, 'gapminder_grid'+str(time.time()), auto_open=False)

    figure = {
        'data': [],
        'layout': {},
        'frames': [],
        'config': {'scrollzoom': True}
    }

    # fill in most of layout
    figure['layout']['xaxis'] = {'title': 'Number of events per month by country', 'gridcolor': '#FFFFFF'}
    figure['layout']['yaxis'] = {'title': 'Emotion charge', 'range':[0,0.25], 'gridcolor': '#FFFFFF'}
    figure['layout']['hovermode'] = 'closest'
    figure['layout']['plot_bgcolor'] = 'rgb(223, 232, 243)'


    figure['layout']['sliders'] = {
        'active': 0,
        'yanchor': 'top',
        'xanchor': 'left',
        'currentvalue': {
            'font': {'size': 20},
            'prefix': 'text-before-value-on-display',
            'visible': True,
            'xanchor': 'right'
        },
        'transition': {'duration': 300, 'easing': 'cubic-in-out'},
        'pad': {'b': 10, 't': 50},
        'len': 0.9,
        'x': 0.1,
        'y': 0,
        'steps': [...]
        }
    {
        'method': 'animate',
        'label': 'label-for-frame',
        'value': 'value-for-frame(defaults to label)',
        'args': [{'frame': {'duration': 300, 'redraw': False},
             'mode': 'immediate'}
        ],
    }
    sliders_dict = {
        'active': 0,
        'yanchor': 'top',
        'xanchor': 'left',
        'currentvalue': {
            'font': {'size': 20},
            'prefix': 'Year:',
            'visible': True,
            'xanchor': 'right'
        },
        'transition': {'duration': 300, 'easing': 'cubic-in-out'},
        'pad': {'b': 10, 't': 50},
        'len': 0.9,
        'x': 0.1,
        'y': 0,
        'steps': []
    }
    figure['layout']['updatemenus'] = [
        {
            'buttons': [
                {
                    'args': [None, {'frame': {'duration': 500, 'redraw': False},
                             'fromcurrent': True, 'transition': {'duration': 300, 'easing': 'quadratic-in-out'}}],
                    'label': 'Play',
                    'method': 'animate'
                },
                {
                    'args': [[None], {'frame': {'duration': 0, 'redraw': False}, 'mode': 'immediate',
                    'transition': {'duration': 0}}],
                    'label': 'Pause',
                    'method': 'animate'
                }
            ],
            'direction': 'left',
            'pad': {'r': 10, 't': 87},
            'showactive': False,
            'type': 'buttons',
            'x': 0.1,
            'xanchor': 'right',
            'y': 0,
            'yanchor': 'top'
        }
    ]          


    col_name_template = '{year}_{continent}_{header}_gapminder_grid'
    year = '2015_02'
    for continent in continents:
        data_dict = {
            'xsrc': grid.get_column_reference(col_name_template.format(
                year=year, continent=continent, header='CountCountrySource'
            )),
            'ysrc': grid.get_column_reference(col_name_template.format(
                year=year, continent=continent, header='NormEvnt'
            )),
            'mode': 'markers',
            'textsrc': grid.get_column_reference(col_name_template.format(
                year=year, continent=continent, header='CountrySource'
            )),
            'marker': {
                'sizemode': 'area',
                'sizeref': 200000,
                'sizesrc': grid.get_column_reference(col_name_template.format(
                        year=year, continent=continent, header='SizeToViz'
                    ))
            },
            'name': continent
        }
        figure['data'].append(data_dict)


    for year in years:
        frame = {'data': [], 'name': str(year)}
        for continent in continents:
            data_dict = {
                'xsrc': grid.get_column_reference(col_name_template.format(
                    year=year, continent=continent, header='CountCountrySource'
                )),
                'ysrc': grid.get_column_reference(col_name_template.format(
                    year=year, continent=continent, header='NormEvnt'
                )),
                'mode': 'markers',
                'textsrc': grid.get_column_reference(col_name_template.format(
                    year=year, continent=continent, header='CountrySource'
                )),
                'marker': {
                    'sizemode': 'area',
                    'sizeref': 200000,
                    'sizesrc': grid.get_column_reference(col_name_template.format(
                        year=year, continent=continent, header='SizeToViz'
                    ))
            },
            'name': continent
            }
            frame['data'].append(data_dict)

        figure['frames'].append(frame)
        slider_step = {'args': [
            [year],
            {'frame': {'duration': 300, 'redraw': False},
             'mode': 'immediate',
           'transition': {'duration': 300}}
         ],
         'label': year,
         'method': 'animate'}
        sliders_dict['steps'].append(slider_step)

    figure['layout']['sliders'] = [sliders_dict]

    py.icreate_animations(figure)


def visualization_Q5(S,all_dicts,Countries_L):
    """ Make visualization for question 5
        Input:
            dataframe and dictionary with list of dictionaries
        Output:
            plotly image
    """

    # list of words that are not readable to avoid
    words_avoid=["ly","verbs","noun","prep","verb","ed","object","polit","quan","vice","neverness","form","econ","coll",
                "pron","modif","acad","be","s","we","freq","ends","qual","indef","gen","conj","period","trait"]

    # initialize lists for x and y data, as well as years and data for the plotly image
    plot_y=[]
    plot_x=[]
    years=[]
    data=[]

    # indexes of countries we want to analize
    Countries_indexes = S.index[S['country_source'].isin(Countries_L)].tolist()

    # initialize counter
    init_country = S["country_source"][Countries_indexes[0]]

    # array of the word positions to analyze
    random_array = [3,50,270]

    # iterate thorugg the indexes
    for n in Countries_indexes:
        numb = n
        # sort the list
        sorted_d = sorted(all_dicts[numb][1].items(), key=operator.itemgetter(1), reverse=True)

        # iterate through array of indexes
        for t in random_array:
            if sorted_d[t][0] in words_avoid:
                pass
            else:
                plot_y.append(sorted_d[t][1]/(all_dicts[numb][0])*100)
                plot_x.append(sorted_d[t][0])
                years.append(S["SizeViz"][n])

        if init_country != S["country_source"][n] or n == len(Countries_indexes) -1:
            trace = go.Scatter(
                    x=plot_x,
                    y=plot_y,
                    name = S["country_source"][n],
                    mode = 'markers',
                    marker = dict(
                        size = years,
                        opacity = 0.5
                    )
                )
            data.append(trace)
            
            plot_y=[]
            plot_x=[]
            years=[]
            
            init_country = S["country_source"][n]
        

    layout = go.Layout(
        legend=dict(x=-.1, y=1,orientation="h"),
        title='Percentage of words used by countries',
        yaxis=dict( title='Percentage of the word in speech'),
        xaxis=dict( title='Words used')
)
    fig = go.Figure(data=data, layout=layout)
    
    return fig