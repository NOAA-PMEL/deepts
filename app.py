from dash import Dash, dcc, html, Input, Output, State, no_update, callback_context, exceptions, CeleryManager, DiskcacheManager
import dash_design_kit as ddk
import plotly.express as px
import plotly.graph_objects as go
import dash_bootstrap_components as dbc
import dash_mantine_components as dmc
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
import celery
from celery import Celery
import diskcache


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


app = app = Dash(__name__, background_callback_manager=background_callback_manager, 
                external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.BOOTSTRAP])
server = app.server  # expose server variable for Procfile
redis_instance = redis.StrictRedis.from_url(os.environ.get("REDIS_URL", "redis://127.0.0.1:6379"))
ESRI_API_KEY = os.environ.get('ESRI_API_KEY')

version = .2

fmt = '%Y-%m-%d %H:%M'

with open("config/sites.json", "r+") as site_file:
    site_json = json.load(site_file)

for site in site_json:
    url = site_json[site]['url']
    info = Info(url)
    start_date, end_date, start_date_seconds, end_date_seconds = info.get_times()
    variables, long_names, units, stardard_names, var_types = info.get_variables()

    time_marks = Info.get_time_marks(start_date_seconds, end_date_seconds)
    if 'start_date' not in site_json[site]:
        site_json[site]['start_date'] = start_date
        site_json[site]['start_date_seconds'] = start_date_seconds
    site_json[site]['end_date'] = end_date
    site_json[site]['end_date_seconds'] = end_date_seconds
    site_json[site]['long_names'] = long_names
    site_json[site]['time_marks'] = time_marks

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


app.layout = html.Div([
    html.Div(id='kick'),
    dcc.Store(id='initial-time-start'),  # time from initial load query string
    dcc.Store(id='initial-time-end'),  # time from initial load query string
    dcc.Store(id='profile-plot-start-time'),
    dcc.Store(id='profile-plot-end-time'),
    dcc.Store(id='ts-plot-start-time'),
    dcc.Store(id='ts-plot-end-time'),
    dcc.Store(id='is-subsampled'),
    dcc.Store(id='site-code'),
    dbc.Navbar(
        dbc.Row(style={'width': '100%'}, align="center", children=[
            dbc.Col(width=2, children=[
                html.Img(src=app.get_asset_url('logo.gif'), style={'height': '97px', 'width': '150px', 'padding-left': '20px'}),
            ]),
            dbc.Col(width=3, style={'display': 'flex', 'align-items': 'left'}, children=[
                dbc.NavbarBrand('Deep Timeseries', className="ml-2", style={'font-size': '2.5em', 'font-weight': 'bold'})
            ]),
            dbc.Col(width=4),
            dbc.Col(width=3, children=[
                dcc.Loading(id='nav-loader', children=[
                    html.Div(id='loading-div'),
                    html.Div(children=[
                        dcc.Loading(dbc.Button("Download Data", id='download-button', className="me-1", disabled=True)),
                        dbc.Modal(children=
                            [
                                dbc.ModalHeader(dbc.ModalTitle("Download Data")),
                                dbc.ModalBody(id='download-body'),
                                dbc.ModalFooter(
                                    dbc.Button(
                                        "Close", id="close-download", className="ms-auto", n_clicks=0
                                    )
                                ),
                            ],
                            id="download-dialog",
                            is_open=False,
                        )
                    ]),
                ])
            ])
        ])
    ),
    dbc.Row(children=[
        dbc.Col(width=4, children=[
            dbc.Card(children=[
                dbc.CardHeader(children=["Select a Variable:"]),
                dbc.CardBody([
                    dbc.RadioItems(id='variable',)
                ])  
            ]),
            dbc.Card(children=[
                dbc.CardHeader(children=['Time range:']),
                dmc.DateRangePicker(
                    id="date-range",
                    description="N.B. Click below, then click the month then year at the top of the dialog to easily set long ranges.",
                    ml=25,
                    style={"width": 330},
                    clearable=False
                ),
            ]),
        ]),
        dbc.Col(width=8, children=[ddk.Graph(id='location-graph')]),
    ]),

    dbc.Row(children=[
        dbc.Card(
            [
                dbc.CardHeader(['Profile Plot', dbc.Button('Resample', disabled=True, id='profile-resample', title='Set time range to match graph and resample.', style={'margin-left': '30px'})]),
                dbc.CardBody(dcc.Loading(ddk.Graph(id='profile-graph', figure=get_blank('Select a site on the map, a variable, and a date range.'))))
            ]
        ),
    ]),
    dbc.Row(children=[
        dbc.Card(style={'bgcolor': '#FFFFFF'}, children=
            [
                dbc.CardHeader(['Timeseries Plot', dbc.Button('Resample', disabled=True, id='ts-resample', title='Set time range to match graph and resample.', style={'margin-left': '30px'})]),
                dbc.CardBody(dcc.Loading(ddk.Graph(id='timeseries-graph', figure=get_blank('Select a site on the map, a variable, and a date range.'))))
            ]
        ),
    ]),
    dbc.Row(style={'margin-bottom': '10px'}, children=[
        dbc.Col(width=12, children=[
            dbc.Card(children=[
                dbc.Row(children=[
                    dbc.Col(width=1, children=[
                        html.Img(src='https://www.pmel.noaa.gov/sites/default/files/PMEL-meatball-logo-sm.png',
                                    height=100,
                                    width=100),
                    ]),
                    dbc.Col(width=10, children=[
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
                            dcc.Link(' Accessibility', href='https://www.pmel.noaa.gov/accessibility')
                        ])
                    ]),
                    dbc.Col(width=1, children=[
                        html.Div(style={'font-size': '1.0em', 'position': 'absolute', 'bottom': '0'},
                                    children=[version])
                    ])
                ])
            ])
        ])
    ])
])


@app.callback(
    Output("download-dialog", "is_open"),
    [Input("download-button", "n_clicks"), Input("close-download", "n_clicks")],
    [State("download-dialog", "is_open")],
)
def toggle_modal(n1, n2, is_open):
    if n1 or n2:
        return not is_open
    return is_open


@app.callback(
    Output('ts-plot-start-time', 'data'),
    Output('ts-plot-end-time', 'data'),
    Output('profile-resample', 'disabled', allow_duplicate=True),
    Output('ts-resample', 'disabled', allow_duplicate=True),
    Input('timeseries-graph', 'relayoutData'),
    State('is-subsampled', 'data'),
    prevent_initial_call=True
)
def relayout_ts(layout_data, in_is_subsampled):
    # Plot is not subsampled to turn off resample now
    if in_is_subsampled is not None and in_is_subsampled == 'no':
        return '', '', True, True
    disabled = True
    if layout_data is not None and 'xaxis.range[0]' in layout_data and 'xaxis.range[1]' in layout_data:
        start = layout_data['xaxis.range[0]']
        end = layout_data['xaxis.range[1]']
        disabled = False
    else:
        return '', '', disabled, disabled
    return start, end, disabled, disabled


@app.callback(
    Output('profile-plot-start-time', 'data'),
    Output('profile-plot-end-time', 'data'),
    Output('profile-resample', 'disabled', allow_duplicate=True),
    Output('ts-resample', 'disabled', allow_duplicate=True),
    Input('profile-graph', 'relayoutData'),
    State('is-subsampled', 'data'),
    prevent_initial_call=True
)
def relayout_ts(layout_data, in_is_subsampled):
    # Plot is not subsampled to turn off resample now
    if in_is_subsampled is not None and in_is_subsampled == 'no':
        return '', '', True, True
    disabled = True
    if layout_data is not None and 'xaxis.range[0]' in layout_data and 'xaxis.range[1]' in layout_data:
        start = layout_data['xaxis.range[0]']
        end = layout_data['xaxis.range[1]']
        disabled = False
    else:
        return '', '', disabled, disabled
    return start, end, disabled, disabled


@app.callback(
    Output('date-range', 'value', allow_duplicate=True),
    Input('ts-resample', 'n_clicks'),
    State('ts-plot-start-time', 'data'),
    State('ts-plot-end-time', 'data'),
    prevent_initial_call=True
)
def set_date_range_from_ts_plot(click, in_time_start, in_time_end):
    if in_time_start is not None and in_time_end is not None:
        dt_start = parser.parse(in_time_start)
        dt_end =parser.parse(in_time_end)
        out_start = dt_start.strftime(constants.d_format)
        out_end = dt_end.strftime(constants.d_format)
        return [out_start, out_end] 
    else:
        return no_update


@app.callback(
    Output('date-range', 'value', allow_duplicate=True),
    Input('profile-resample', 'n_clicks'),
    State('profile-plot-start-time', 'data'),
    State('profile-plot-end-time', 'data'),
    prevent_initial_call=True
)
def set_date_range_from_profile_plot(click, in_time_start, in_time_end):
    if in_time_start is not None and in_time_end is not None:
        print(in_time_start)
        print(in_time_end)
        dt_start = parser.parse(in_time_start)
        dt_end = parser.parse(in_time_end)
        out_start = dt_start.strftime(constants.d_format)
        out_end = dt_end.strftime(constants.d_format)
        return [out_start, out_end] 
    else:
        return no_update        


@app.callback(
    [
        Output('site-code', 'data'),
        Output('date-range', 'value', allow_duplicate=True),
        Output('date-range', 'minDate'),
        Output('date-range', 'maxDate'),
        Output('variable','options'),
        Output('variable', 'value'),
        Output('initial-time-start', 'data'),
        Output('initial-time-end', 'data')
    ],
    [
        Input('location-graph', 'clickData')
    ], prevent_initial_call=True
)
def set_selected_site(in_click):
    if in_click is not None:
        vops = []
        point = in_click['points'][0]
        site = point['customdata']
        short_names = site_json[site]['variables']
        for var in short_names:
            if var != 'PRES':
                vops.append({'label': site_json[site]['long_names'][var], 'value': var})
        return [site, 
                [site_json[site]['start_date'], 
                site_json[site]['end_date']],
                site_json[site]['start_date'], 
                site_json[site]['end_date'],
                vops,
                short_names[-1],
                site_json[site]['start_date_seconds'], 
                site_json[site]['end_date_seconds'], 
                ]
    return no_update


@app.callback(
    [
        Output('location-graph', 'figure'),
    ],
    [
        Input('kick', 'n_clicks'),
        Input('site-code', 'data')
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
                                marker={'color': 'black', 'size': 15},
                                mode='markers')
    figure.add_trace(black_trace)
    if in_site_code is not None:
        yellow_df = locations_df.loc[locations_df['site_code'] == in_site_code]
        yellow_trace = go.Scattermapbox(lat=yellow_df['latitude'].values,
                            lon=yellow_df['longitude'].values,
                            hovertext=[in_site_code],
                            hoverinfo='lat+lon+text',
                            customdata=[in_site_code],
                            marker={'color': 'yellow', 'size': 15},
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
                    "https://ibasemaps-api.arcgis.com/arcgis/rest/services/Ocean/World_Ocean_Base/MapServer/tile/{z}/{y}/{x}?token=" + ESRI_API_KEY
                ]
            }
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
        Output('profile-graph', 'figure'),
        Output('timeseries-graph', 'figure'),
        Output('download-body', 'children'),
        Output('download-button', 'disabled'),
        Output('profile-resample', 'disabled'),
        Output('ts-resample', 'disabled'),
        Output('is-subsampled', 'data')
    ],
    [
        Input('site-code', 'data'),
        Input('variable', 'value'),
        Input('date-range', 'value'),
    ], background=True, manager=background_callback_manager, prevent_initial_call=True
)
def update_plots(in_site, in_variable, p_in_dates,):
    print('plots == ' + str((in_site, in_variable, p_in_dates)))
    is_subsampled = 'no'
    if in_site is None or p_in_dates is None:
        return [get_blank('Select a site on the map, a variable, and a date range.'), get_blank('Select a site on the map, a variable, and a date range.'), list_group, False, True, True, is_subsampled]
    variables = site_json[in_site]['variables'].copy()
    variables.append('time')
    variables.append('depth')
    variables.append('site_code')
    url = site_json[in_site]['url']
    list_group = html.Div()
    list_group.children = []
    link_group = dbc.ListGroup(horizontal=True)
    link_group.children = []
    list_group.children.append(link_group)
    meta_item = dbc.ListGroupItem(in_site + ': ', href=url, target='_blank')
    link_group.children.append(meta_item)

    p_startdt = datetime.datetime.strptime(p_in_dates[0], constants.d_format)
    p_enddt = datetime.datetime.strptime(p_in_dates[1], constants.d_format)
    time_range = p_enddt - p_startdt
    num_days = time_range.days
    num_hours = num_days*24
    if in_variable is None or len(in_variable) == 0:
        return [get_blank('Select a site on the map, a variable, and a date range.'), get_blank('Select a site on the map, a variable, and a date range.'), list_group, False, True, True, is_subsampled]


    if in_variable == 'PSAL':
        cs=px.colors.sequential.Viridis
    else:
        cs=px.colors.sequential.Inferno    
    get_vars = ','.join(variables)
    time_con = '&time>='+p_in_dates[0]+'&time<='+p_in_dates[1]
    # Estimate the size and sample accordingly
    p_title = in_variable + ' at ' + in_site
    ts_title = 'Timeseries of ' + in_variable + ' at ' + in_site + ' colored by depth'
    if 'obs_per_hour' in site_json[in_site]:
        num_obs = site_json[in_site]['obs_per_hour'] * num_hours
        factor = num_obs/10_000
        if factor > .16:
            if factor < 1:
                minutes = int(factor*60)
                if minutes > 15:
                    time_con = time_con + '&orderByClosest("depth,time/'+str(factor)+'hour")'
            else:
                time_con = time_con + '&orderByClosest("depth,time/'+str(factor)+'hour")'
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
        depth_con = '&depth>' + str(site_json[in_site]['minimum_depth'])
    
    p_url = url
    p_url = p_url + '.csv?' + get_vars + time_con + depth_con
    item = dbc.ListGroupItem('.html', href=p_url.replace('.csv', '.htmlTable'), target='_blank')
    link_group.children.append(item)
    item = dbc.ListGroupItem('.csv', href=p_url.replace('.htmlTable', '.csv'), target='_blank')
    link_group.children.append(item)
    item = dbc.ListGroupItem('.nc', href=p_url.replace('.csv', '.ncCF'), target='_blank')
    link_group.children.append(item)
    if 'orderByClosest' in p_url:
        is_subsampled = 'yes'
    df = pd.read_csv(p_url, skiprows=[1])
    figure = px.scatter(df, x='time', y='PRES', color=in_variable, color_continuous_scale=cs, title=p_title, hover_data={'time':':%Y-%m-%dT%H:%M:%S'})
    figure.update_layout(font=dict(size=16), modebar=dict(orientation='h'), paper_bgcolor="white", plot_bgcolor='white')
    figure.update_xaxes({
        'title': 'Time',
        'titlefont': {'size':16},
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
    })
    figure.update_yaxes({
        'autorange': 'reversed',
        'title': 'PRES (dbar)',
        'titlefont': {'size': 16},
        'gridcolor': line_rgb,
        'zeroline': True,
        'zerolinecolor': line_rgb,
        'showline': True,
        'linewidth': 1,
        'linecolor': line_rgb,
        'mirror': True,
        'tickfont': {'size': 14}
    })
    df.loc[:, 'time'] = pd.to_datetime(df['time'])
    df = Info.plug_gaps(df, 'time', 'depth', ['latitude', 'longitude', 'site_code', 'depth'], 1.25)
    ts = go.Figure()
    for idx, d in enumerate(df['depth'].unique()):
        pdf = df.loc[df['depth'] == d].copy()  
        pdf.loc[:, 'texttime'] = pdf.loc[:,'time'].dt.strftime(fmt)
        pdf.loc[:,'text'] = 'Time: ' + pdf.loc[:,'texttime'] + '<br>' + in_variable + ': ' + pdf.loc[:,in_variable].astype(str) + '<br>at Depth: ' + str(d)
        pts = go.Scattergl(mode='lines', x=pdf['time'], y=pdf[in_variable], hoverinfo='text', hovertext=pdf['text'], showlegend=True, name=str(d) , line=dict(color=cc.b_glasbey_bw_minc_20[idx]))
        ts.add_traces(pts)
    ts.update_layout(legend=dict(orientation="v", yanchor="top", y=.97, xanchor="right", x=1.08, bgcolor='white'), 
                     plot_bgcolor=plot_bg, 
                     font=dict(size=16),
                     modebar=dict(orientation='h'),
                     paper_bgcolor="white",
                     )
    ts.update_traces(connectgaps=False)
    ts.update_xaxes({
        'title': 'Time',
        'titlefont': {'size':16},
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
    })
    ts.update_yaxes({
        'title': in_variable,
        'titlefont': {'size': 16},
        'gridcolor': line_rgb,
        'zeroline': True,
        'zerolinecolor': line_rgb,
        'showline': True,
        'linewidth': 1,
        'linecolor': line_rgb,
        'mirror': True,
        'tickfont': {'size': 14}
    })
    ts.update_layout(showlegend=True, title=ts_title)
    print('Plots from: ' + p_url)
    #                               download enabled, but resample disabled until you zoom the plot, is_subsampled='yes' required for it to turn on later
    return [figure, ts, list_group, False, True, True, is_subsampled]


if __name__ == '__main__':
    app.run_server(debug=True)
