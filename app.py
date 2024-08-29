from dash_enterprise_libraries import EnterpriseDash
from dash import dcc, get_asset_url, html, Input, Output, State, no_update, callback_context, exceptions, CeleryManager, DiskcacheManager
import dash_design_kit as ddk
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import redis
import os
import datetime
from dateutil import parser
import constants
import db
import json
from sdig.erddap.info import Info
import colorcet as cc
from celery import Celery
import diskcache
import numpy as np
import ssl
import theme
ssl._create_default_https_context = ssl._create_unverified_context

d_format = "%Y-%m-%d"

if os.environ.get("DASH_ENTERPRISE_ENV") == "WORKSPACE":
    # For testing...
    # import diskcache
    cache = diskcache.Cache("./cache")
    background_callback_manager = DiskcacheManager(cache)
else:
    # For production...
    celery_app = Celery(__name__, broker=os.environ['REDIS_URL'], backend=os.environ['REDIS_URL'])
    background_callback_manager = CeleryManager(celery_app)

line_rgb = 'rgba(.04,.04,.04,.2)'
plot_bg = 'rgba(1.0, 1.0, 1.0 ,1.0)'


app = app = EnterpriseDash(__name__, background_callback_manager=background_callback_manager)

app.setup_shortcuts(
    logo=get_asset_url('logo.gif'),
    title="OceanSITES Deep Timeseries", # Default: app.title
    size="normal" # Can also be "slim"
)
server = app.server  # expose server variable for Procfile
redis_instance = redis.StrictRedis.from_url(os.environ.get("REDIS_URL", "redis://127.0.0.1:6379"))
ESRI_API_KEY = os.environ.get('ESRI_API_KEY')

version = ' Version v2.0'

fmt = '%Y-%m-%d %H:%M'
fmtz = '%Y-%m-%d %H:%M:%sZ'

with open("config/sites.json", "r+") as site_file:
    config = json.load(site_file)

site_json = config['sites']

site_options = []

for site in config['sites']:
    site_options.append({'label': site, 'value': site})

all_start_seconds = config['all_start_seconds']
all_end_seconds = config['all_end_seconds']

all_starto = datetime.datetime.fromtimestamp(all_start_seconds)
all_start = datetime.datetime.strftime(all_starto, d_format)
all_endo = datetime.datetime.fromtimestamp(all_end_seconds)
all_end = datetime.datetime.strftime(all_endo, d_format)

time_marks = Info.get_time_marks(all_start_seconds, all_end_seconds)

month_step = 60*60*24*30.25
obs_day = 12*21
observation_count = 13657673


def get_blank(message):
    blank_graph = go.Figure(go.Scatter(x=[0, 1], y=[0, 1], showlegend=False))
    blank_graph.add_trace(go.Scatter(x=[0, 1], y=[0, 1], showlegend=False))
    blank_graph.update_traces(visible=False)
    blank_graph.update_layout(
        xaxis={"visible": False},
        yaxis={"visible": False},
        annotations=[
            {
                "text": message,
                "xref": "paper",
                "yref": "paper",
                "showarrow": False,
                "font": {
                    "size": 14
                }
            },
        ]
    )
    return blank_graph


app.layout = ddk.App(theme=theme.theme, children=[
    html.Div(id='kick'),
    dcc.Store(id='xrange'),
    dcc.Store(id='is-subsampled'),
    ddk.Card(width=.3, children=[
        ddk.Modal(target_id='data-download', hide_target=True, children=[
            dcc.Loading(html.Button("Download Data", id='download-button', disabled=True))
        ]),
        ddk.Card(children=[
            dcc.RadioItems(id='variable', options = [
                {'label': 'Conductivity', 'value': 'CNDC'},
                {'label': 'Disolved Oxygen', 'value': 'DOX2'},
                {'label': 'Salinity', 'value': 'PSAL'},
                {'label': 'Temperature', 'value': 'TEMP'}
            ])
        ]),
        ddk.Card(children=[
            ddk.Block(width=.5, children=[
                dcc.Input(id='start-date', debounce=True, value=all_start),
            ]),
            ddk.Block(width=.5, children=[
                dcc.Input(id='end-date', debounce=True, value=all_end),
            ]),
            html.Div(style={'padding-right': '40px', 'padding-left': '40px', 'padding-top': '20px', 'padding-bottom': '45px'}, children=[
                    dcc.RangeSlider(id='time-range-slider',
                                    value=[all_start_seconds, all_end_seconds],
                                    min=all_start_seconds,
                                    max=all_end_seconds,
                                    step=month_step,
                                    marks=time_marks,
                                    updatemode='mouseup',
                                    allowCross=False)
            ])
        ]),
        ddk.Card(children=[
            dcc.Dropdown(id='site', options=site_options)
        ]),
    ]),
    ddk.Card(width=.7, children=[ddk.Graph(id='location-graph')]),
    ddk.Card(width=1, children=
        [
            ddk.CardHeader([html.Button('Resample', disabled=True, id='resample', title='Set time range to match graph and resample.', style={'margin-left': '30px'})]),
            dcc.Loading(ddk.Graph(id='graph', figure=get_blank('Select a site on the map, a variable, and a date range.')))
        ]
    ),
    ddk.Card(style={'margin-bottom': '10px'}, children=[
        ddk.Block(children=[
            ddk.Block(width=.08, children=[
                html.Img(src='https://www.pmel.noaa.gov/sites/default/files/PMEL-meatball-logo-sm.png',
                            height=100,
                            width=100),
            ]),
            ddk.Block(width=.83, children=[
                html.Div(children=[
                    dcc.Link('National Oceanic and Atmospheric Administration',
                                href='https://www.noaa.gov/'),
                ]),
                html.Div(children=[
                    dcc.Link('Pacific Marine Environmental Laboratory', href='https://www.pmel.noaa.gov/'),
                ]),
                html.Div(children=[
                    dcc.Link('oar.pmel.webmaster@noaa.gov', href='mailto:oar.pmel.webmaster@noaa.gov')
                ]),
                html.Div(children=[
                    dcc.Link('DOC |', href='https://www.commerce.gov/'),
                    dcc.Link(' NOAA |', href='https://www.noaa.gov/'),
                    dcc.Link(' OAR |', href='https://www.research.noaa.gov/'),
                    dcc.Link(' PMEL |', href='https://www.pmel.noaa.gov/'),
                    dcc.Link(' Privacy Policy |', href='https://www.noaa.gov/disclaimer'),
                    dcc.Link(' Disclaimer |', href='https://www.noaa.gov/disclaimer'),
                    dcc.Link(' Accessibility |', href='https://www.pmel.noaa.gov/accessibility'),
                    dcc.Link( version, href='https://github.com/NOAA-PMEL/de_osmc')
                ])
            ]),
        ]),
    ]),
    ddk.Card(id='data-download', children=[
        ddk.CardHeader('Download Data'),
        html.Div(id='download-body')
    ])
])


@app.callback(
[
    Output('xrange', 'data'),
    Output('resample', 'disabled', allow_duplicate=True),
],
[
    Input('graph', 'relayoutData'),
],
[
    State('is-subsampled', 'data'),
],prevent_initial_call=True
)
def relayout(layout_data, in_is_subsampled):
    # Plot is not subsampled to turn off resample now
    if in_is_subsampled is not None and in_is_subsampled == 'no':
        return [no_update, no_update]
    disabled = True
    if layout_data is not None and 'xaxis.range[0]' in layout_data and 'xaxis.range[1]' in layout_data:
        start = layout_data['xaxis.range[0]']
        end = layout_data['xaxis.range[1]']
        xrange = json.dumps([start, end])
        disabled = False
    else:
        return [no_update, no_update]
    return [xrange, disabled]


@app.callback(
[
    Output('time-range-slider', 'value', allow_duplicate=True),
],
[
    Input('resample', 'n_clicks'),
],
[
    State('xrange', 'data')
], prevent_initial_call=True
)
def set_date_range_from_plot(click, in_xrange):
    if in_xrange is not None:
        xrange = json.loads(in_xrange)
        dt_start = parser.isoparse(xrange[0])
        dt_end = parser.isoparse(xrange[1])
        out_start = dt_start.timestamp()
        out_end = dt_end.timestamp()
        return [[out_start, out_end]]
    else:
        raise exceptions.PreventUpdate


@app.callback(
    [
        Output('site', 'value'),
        Output('start-date', 'value', allow_duplicate=True),
        Output('end-date', 'value', allow_duplicate=True),
        Output('variable', 'options'),
        Output('variable', 'value'),
        Output('time-range-slider', 'value'),
    ],
    [
        Input('location-graph', 'clickData'),
        Input('site', 'value')
    ], prevent_initial_call=True
)
def set_selected_site(in_click, in_site):
    site = None
    trigger_id = callback_context.triggered_id
    if in_click is not None and trigger_id =='location-graph':
        point = in_click['points'][0]
        site = point['customdata']
    elif in_site is not None and trigger_id == 'site':
        site = in_site
    vops = [
        {'label': 'Conductivity', 'value': 'CNDC', 'disabled': True},
        {'label': 'Disolved Oxygen', 'value': 'DOX2', 'disabled': True},
        {'label': 'Salinity', 'value': 'PSAL', 'disabled': True},
        {'label': 'Temperature', 'value': 'TEMP', 'disabled': True}
    ]
    var_value = None
    if site is not None:
        short_names = site_json[site]['variables']
        for var in short_names:
            for op in vops:
                if var == op['value']:
                    if var_value is None:
                        var_value = var
                    op['disabled'] = False
        return [site, 
                site_json[site]['start_date'], 
                site_json[site]['end_date'],
                vops,
                var_value,
                [site_json[site]['start_date_seconds'], 
                site_json[site]['end_date_seconds'],]
                ]
    return no_update


@app.callback(
    [
        Output('location-graph', 'figure'),
    ],
    [
        Input('kick', 'n_clicks'),
        Input('site', 'value')
    ]
)
def update_location_map(kick, in_site_code):
    locations_df = db.get_locations()
    figure = go.Figure()
    black_trace = go.Scattermapbox(lat=locations_df['latitude'].values,
                                lon=locations_df['longitude'].values,
                                hovertext=locations_df['site_code'],
                                hoverinfo='lat+lon+text',
                                customdata=locations_df['site_code'],
                                marker={'color': 'black', 'size': 10},
                                mode='markers')
    figure.add_trace(black_trace)
    if in_site_code is not None:
        yellow_df = locations_df.loc[locations_df['site_code'] == in_site_code]
        yellow_trace = go.Scattermapbox(lat=yellow_df['latitude'].values,
                            lon=yellow_df['longitude'].values,
                            hovertext=[in_site_code],
                            hoverinfo='lat+lon+text',
                            customdata=[in_site_code],
                            marker={'color': 'yellow', 'size': 10},
                            mode='markers')
        figure.add_trace(yellow_trace)
    figure.update_layout(
        showlegend=False,
        mapbox_style="white-bg",
        mapbox_layers=[
            {
                "below": 'traces',
                "sourcetype": "raster",
                "sourceattribution": "Powered by Esri",
                "source": [
                    'https://ibasemaps-api.arcgis.com/arcgis/rest/services/Ocean/World_Ocean_Base/MapServer/tile/{z}/{y}/{x}?token=' + ESRI_API_KEY
                ]
            },
        ],
        mapbox_zoom=1.1,
        mapbox_center={'lat': locations_df['latitude'].mean(), 'lon': locations_df['longitude'].mean()},
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        legend=dict(
            orientation="v",
            x=-.01,
        ),
        modebar_orientation='v',
    )
    
    return [figure]


@app.callback(
    [
        Output('graph', 'figure'),
        Output('download-body', 'children'),
        Output('download-button', 'disabled'),
        Output('resample', 'disabled'),
        Output('is-subsampled', 'data')
    ],
    [
        Input('site', 'value'),
        Input('variable', 'value'),
        Input('time-range-slider', 'value'),
    ], background=True, manager=background_callback_manager, prevent_initial_call=True
)
def update_plots(in_site, in_variable, p_slider_values):
    print('plotting fired')
    is_subsampled = 'no'
    if in_site is None or p_slider_values is None:
        return [get_blank('Select a site on the map, a variable, and a date range.'), '', True, True, is_subsampled]
    variables = site_json[in_site]['variables'].copy()
    variables.append('time')
    bottom_title = site_json[in_site]['title']
    if site_json[in_site]['has_depth'] == "true":
        variables.append(site_json[in_site]['depth_name'])
    variables.append('site_code')
    variables.append('latitude')
    variables.append('longitude')
    variables.append('id')
    long_names = site_json[in_site]['long_names']
    units = site_json[in_site]['units']

    url = site_json[in_site]['url']
    list_group = html.Div()
    list_group.children = []
    meta_item = dcc.Link('ERDDAP data page for '+ in_site, href=url, target='_blank')
    list_group.children.append(meta_item)
    list_group.children.append(html.P(children=["Click a format button below to download the full resolution of the data for the selected date range."],
                                      style={'margin-top': '30px'}))

    p_startdt = datetime.datetime.fromtimestamp(p_slider_values[0])
    p_enddt = datetime.datetime.fromtimestamp(p_slider_values[1])
    p_start_date = p_startdt.strftime(d_format)
    p_end_date = p_enddt.strftime(d_format)
    time_range = p_enddt - p_startdt
    num_days = time_range.days
    num_hours = num_days*24
    if in_variable is None or len(in_variable) == 0:
        return [get_blank('NO VARIABLE Select a site on the map, a variable, and a date range.'), get_blank('Select a site on the map, a variable, and a date range.'), list_group, False, True, True, is_subsampled]
    if in_variable == 'PSAL':
        cs=px.colors.sequential.Viridis
    else:
        cs=px.colors.sequential.Inferno    
    get_vars = ','.join(variables)
    time_con = '&time>='+p_start_date+'&time<='+p_end_date
    # Estimate the size and sample accordingly
    
    if in_variable in long_names:
        var_units = long_names[in_variable]
    else:
        var_units = in_variable

    if in_variable in units:
        var_units = var_units + ' (' + units[in_variable] + ')'
    
    p_title = var_units + ' at ' + in_site
    ts_title = var_units + ' at ' + in_site + ' colored by ID'

    if in_variable in units:
        short_label = in_variable + ' (' + units[in_variable] + ')'
    else:
        short_label = in_variable
            
    order_by = ''
    if 'obs_per_hour' in site_json[in_site]:
        num_obs = site_json[in_site]['obs_per_hour'] * num_hours
        factor = num_obs/10_000
        if factor > .16:
            if factor < 1:
                minutes = int(factor*60)
                if minutes > 15:
                    if site_json[in_site]['has_depth'] == "true":
                        d_name = site_json[in_site]['depth_name']
                        order_by = '&orderByClosest("id,' +d_name +',time/'+str(factor)+'hour")'
                    else:
                        order_by = '&orderByClosest("id,time/'+str(factor)+'hour")'
            else:
                if site_json[in_site]['has_depth'] == "true":
                    d_name = site_json[in_site]['depth_name']
                    order_by = '&orderByClosest("id,' + d_name + ',time/'+str(factor)+'hour")'
                else:
                    order_by = '&orderByClosest("id,time/'+str(factor)+'hour")'

  
        if factor < 1 and factor > .16:
            minutes = int(factor*60)
            if minutes > 15:
                p_title = p_title + ' (sampled every ' + str(int(factor*60)) + ' minutes)'
                ts_title = ts_title + ' (sampled every ' + str(int(factor*60)) + ' minutes)'  
        elif factor >= 1 and factor <= 24:
            p_title = p_title + ' (sampled every ' + str(int(factor)) + ' hours)'
            ts_title = ts_title + ' (sampled every ' + str(int(factor)) + ' hours)'
        elif factor > 24:
            p_title = p_title + ' (sampled every ' + str(int(factor/24)) + ' days)'
            ts_title = ts_title + ' (sampled every ' + str(int(factor/24)) + ' days)'
    # Use minimum depth if defined
    depth_con = ''
    if 'minimum_depth' in site_json[in_site]:
        d_name = site_json[in_site]['depth_name']
        depth_con = '&' + d_name + '>' + str(site_json[in_site]['minimum_depth'])
    p_url = url
    p_url = p_url + '.csv?' + get_vars + depth_con + time_con
    item = dcc.Link('.html', href=p_url.replace('.csv', '.htmlTable'), target='_blank')
    list_group.children.append(item)
    item = dcc.Link('.csv', href=p_url.replace('.htmlTable', '.csv'), target='_blank')
    list_group.children.append(item)
    item = dcc.Link('.nc', href=p_url.replace('.csv', '.ncCF'), target='_blank')
    list_group.children.append(item)
    if 'orderByClosest' in order_by:
        is_subsampled = 'yes'
    p_url = p_url + order_by
    print('getting data  ', p_url)
    df = pd.read_csv(p_url, skiprows=[1])
    if 'multiple_depths' in site_json[in_site]:
        # Data set has multiple depths per ID so the timeseries will backtrack on itself with at the end of the section.
        # Other data sets are one nominal depth per ID
        # This makes a new ID that is the combination of the old ID and the depth
        d_name = site_json[in_site]['depth_name']
        df['id'] = df['id'] + '_' +df[d_name].astype(str)
        # This code inserts a NaN at the depth changes so the plots does not double back on itself, but
        # the above is better since you can click on and off the depths like other data sets
        # cols = df.columns
        # df['mask'] = df['depth'].ne(df['depth'].shift(-1))
        # df_dup = df[df['mask']].copy()
        # df_dup[in_variable] = np.nan
        # df_dup.index += .5
        # df = pd.concat([df_dup, df])
        # df = df.sort_index()
        # df = df[cols]
        # df = df.reset_index()
    hover_vars = ["time", in_variable, "id"]
    if site_json[in_site]['has_pressure'] == "true":
        hover_vars.append(site_json[in_site]['pressure_name'])
        y_var = site_json[in_site]['pressure_name']
    else:
        y_var = site_json[in_site]['depth_name']

    if site_json[in_site]['has_depth'] == 'true':
        hover_vars.append(site_json[in_site]['depth_name'])
        
    df['text_time'] = df['time'].astype(str)
    df['text'] = '<b>' + in_variable + '=' + df[in_variable].apply(lambda x: '{0:.2f}'.format(x)) + '</b>' \
                    + '<br>' + df['text_time'] + '<br>'\
                    + y_var + '=' + df[y_var].astype(str)
    if site_json[in_site]['has_depth'] == "true":
        d_name = site_json[in_site]['depth_name']
        df['text'] = df['text'] + '<br>' + d_name + '=' + df[d_name].astype(str)
    df = df.sort_values(by=['time'], ascending=True)
    trace = go.Scattergl(x=df['time'], y=df[y_var],
                        connectgaps=False,
                        name=in_variable,
                        mode='markers',
                        hovertext=df['text'],
                        marker=dict(
                            cmin=df[in_variable].min(),
                            cmax=df[in_variable].max(),
                            color=df[in_variable],
                            colorscale=cs,
                            colorbar=dict(
                                title_side='right',
                                title_font_size=16,
                                tickfont_size=16,
                                title_text=short_label,
                                y=.2,
                                len=.48
                            )
                        ),
                        hoverinfo="text",
                        hoverlabel=dict(namelength=-1),
                        showlegend=False,
                        )

    df.loc[:, 'time'] = pd.to_datetime(df['time'])
    df = Info.plug_gaps(df, 'time', 'id', ['latitude', 'longitude', 'site_code', 'id'], 1.25)
    ts_traces = []
    for idx, d in enumerate(df['id'].unique()):
        pdf = df.loc[df['id'] == d].copy()  
        pdf.loc[:, 'texttime'] = pdf.loc[:,'time'].dt.strftime(fmt)
        if site_json[in_site]['has_depth'] == 'true':
            pdf.loc[:,'text'] = 'Time: ' + pdf.loc[:,'texttime'] + '<br>' + in_variable + ': ' + pdf.loc[:,in_variable].astype(str) + '<br>for file: ' + str(d) + '<br> at depth: ' + pdf.loc[:,site_json[in_site]['depth_name']].astype(str)
        else:
            pdf.loc[:,'text'] = 'Time: ' + pdf.loc[:,'texttime'] + '<br>' + in_variable + ': ' + pdf.loc[:,in_variable].astype(str) + '<br>for file: ' + str(d)   
        pts = go.Scattergl(mode='lines', x=pdf['time'], y=pdf[in_variable], hoverinfo='text', hovertext=pdf['text'], showlegend=True, name=str(d) , line=dict(color=cc.b_glasbey_bw_minc_20[idx]))
        ts_traces.append(pts)
    
    print('Making plots from: ' + p_url)
    sub_plots = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[450, 450])
    for ts_trace in ts_traces:
        sub_plots.add_trace(ts_trace, row=1, col=1)
    sub_plots.add_trace(trace, row=2, col=1)
    sub_plots.update_layout(legend=dict(title='ID', orientation="v", yanchor="top", y=1.1, xanchor="right", x=1.08, bgcolor='white', font_size=16), plot_bgcolor=plot_bg, 
                            modebar=dict(orientation='h'), paper_bgcolor="white", margin=dict(l=80, r=80, b=120, t=80),
                            font=dict(size=18), title=ts_title, title_x=.05, title_y=.97, showlegend=True, height=950,
                            )
    sub_plots.update_xaxes({
        'ticklabelmode': 'period',
        'showticklabels': True,
        'gridcolor': line_rgb,
        'zeroline': True,
        'zerolinecolor': line_rgb,
        'showline': True,
        'linewidth': 1,
        'linecolor': line_rgb,
        'mirror': True,
        'tickfont': {'size': 16},
        'tickformatstops' : [
            dict(dtickrange=[1000, 60000], value="%H:%M:%S\n%d%b%Y"),
            dict(dtickrange=[60000, 3600000], value="%H:%M\n%d%b%Y"),
            dict(dtickrange=[3600000, 86400000], value="%H:%M\n%d%b%Y"),
            dict(dtickrange=[86400000, 604800000], value="%e\n%b %Y"),
            dict(dtickrange=[604800000, "M1"], value="%b\n%Y"),
            dict(dtickrange=["M1", "M12"], value="%b\n%Y"),
            dict(dtickrange=["M12", None], value="%Y")
        ]
    }, row=1, col=1)
    sub_plots.update_yaxes({
        'title': short_label,
        'titlefont': {'size': 16},
        'gridcolor': line_rgb,
        'zeroline': True,
        'zerolinecolor': line_rgb,
        'showline': True,
        'linewidth': 1,
        'linecolor': line_rgb,
        'mirror': True,
        'tickfont': {'size': 16}
    }, row=1, col=1)
    sub_plots.add_annotation(
        xref='x domain',
        yref='y domain',
        xanchor='right',
        yanchor='bottom',
        x=1.0,
        y=-.3,
        font_size=22,
        text=bottom_title,
        showarrow=False,
        bgcolor='rgba(255,255,255,.85)', row=2, col=1
    )
    sub_plots.update_xaxes({
        'title': None,
        'titlefont': {'size':18},
        'ticklabelmode': 'period',
        'showticklabels': True,
        'gridcolor': line_rgb,
        'zeroline': True,
        'zerolinecolor': line_rgb,
        'showline': True,
        'linewidth': 1,
        'linecolor': line_rgb,
        'mirror': True,
        'tickfont': {'size': 16},
        'tickformatstops' : [
            dict(dtickrange=[1000, 60000], value="%H:%M:%S\n%d%b%Y"),
            dict(dtickrange=[60000, 3600000], value="%H:%M\n%d%b%Y"),
            dict(dtickrange=[3600000, 86400000], value="%H:%M\n%d%b%Y"),
            dict(dtickrange=[86400000, 604800000], value="%e\n%b %Y"),
            dict(dtickrange=[604800000, "M1"], value="%b\n%Y"),
            dict(dtickrange=["M1", "M12"], value="%b\n%Y"),
            dict(dtickrange=["M12", None], value="%Y")
        ]
    }, row=2, col=1)
    p_name = site_json[in_site]['pressure_name'] + ' *dbar'
    sub_plots.update_yaxes({
        'autorange': 'reversed',
        'title': p_name,
        'titlefont': {'size': 18},
        'gridcolor': line_rgb,
        'zeroline': True,
        'zerolinecolor': line_rgb,
        'showline': True,
        'linewidth': 1,
        'linecolor': line_rgb,
        'mirror': True,
        'tickfont': {'size': 16}
    }, row=2, col=1)

    return [sub_plots, list_group, False, True, is_subsampled]


@app.callback(
    [
        Output('time-range-slider', 'value', allow_duplicate=True),
        Output('start-date', 'value', allow_duplicate=True),
        Output('end-date', 'value', allow_duplicate=True)
    ],
    [
        Input('time-range-slider', 'value'),
        Input('start-date', 'value'),
        Input('end-date', 'value'),
    ], prevent_initial_call=True
)
def set_date_range_from_slider(slide_values, in_start_date, in_end_date,):
    if slide_values is None or len(slide_values) <= 0:
        raise exceptions.PreventUpdate
    if in_start_date is None or len(in_start_date) <= 0:
        raise exceptions.PreventUpdate
    if in_end_date is None or len(in_end_date) <= 0:
        raise exceptions.PreventUpdate

    range_min = all_start_seconds
    range_max = all_end_seconds

    trigger_id = callback_context.triggered_id

    start_seconds = slide_values[0]
    end_seconds = slide_values[1]

    start_output = in_start_date
    end_output = in_end_date

    if trigger_id == 'start-date':
        try:
            in_start_date_obj = datetime.datetime.strptime(in_start_date, d_format)
        except:
            in_start_date_obj = datetime.datetime.fromtimestamp(start_seconds)
        start_output = in_start_date_obj.date().strftime(d_format)
        start_seconds = in_start_date_obj.timestamp()
        if start_seconds < range_min:
            start_seconds = range_min
            in_start_date_obj = datetime.datetime.fromtimestamp(start_seconds)
            start_output = in_start_date_obj.date().strftime(d_format)
        elif start_seconds > range_max:
            start_seconds = range_max
            in_start_date_obj = datetime.datetime.fromtimestamp(start_seconds)
            start_output = in_start_date_obj.date().strftime(d_format)
        elif start_seconds > end_seconds:
            start_seconds = end_seconds
            in_start_date_obj = datetime.datetime.fromtimestamp(start_seconds)
            start_output = in_start_date_obj.date().strftime(d_format)
        return [no_update, start_output, no_update]
    elif trigger_id == 'end-date':
        try:
            in_end_date_obj = datetime.datetime.strptime(in_end_date, d_format)
        except:
            in_end_date_obj = datetime.datetime.fromtimestamp((end_seconds))
        end_output = in_end_date_obj.date().strftime(d_format)
        end_seconds = in_end_date_obj.timestamp()
        if end_seconds < range_min:
            end_seconds = range_min
            in_end_date_obj = datetime.datetime.fromtimestamp(end_seconds)
            end_output = in_end_date_obj.date().strftime(d_format)
        elif end_seconds > range_max:
            end_seconds = range_max
            in_end_date_obj = datetime.datetime.fromtimestamp(end_seconds)
            end_output = in_end_date_obj.date().strftime(d_format)
        elif end_seconds < start_seconds:
            end_seconds = start_seconds
            in_end_date_obj = datetime.datetime.fromtimestamp(end_seconds)
            end_output = in_end_date_obj.date().strftime(d_format)
        return [no_update, no_update, end_output]
    elif trigger_id == 'time-range-slider':
        in_start_date_obj = datetime.datetime.fromtimestamp(slide_values[0])
        start_output = in_start_date_obj.strftime(d_format)
        in_end_date_obj = datetime.datetime.fromtimestamp(slide_values[1])
        end_output = in_end_date_obj.strftime(d_format)
        return [no_update, start_output, end_output]
    return no_update, no_update, no_update

if __name__ == '__main__':
    app.run_server(debug=True, dev_tools_props_check=False)
