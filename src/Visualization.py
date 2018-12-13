import plotly.graph_objs as go
import pandas as pd
import math
import numpy as np
import cufflinks as cf
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
from IPython.display import display, HTML

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
