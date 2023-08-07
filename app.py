from dash import Dash, dcc, html, Input, Output, State, no_update, callback_context
import dash_design_kit as ddk
import plotly.express as px
import plotly.graph_objects as go
import dash_bootstrap_components as dbc
import pandas as pd
import redis
import os
import datetime
import constants
import db
from sdig.erddap.info import Info

app = app = Dash(__name__,
                external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.BOOTSTRAP])
server = app.server  # expose server variable for Procfile
redis_instance = redis.StrictRedis.from_url(os.environ.get("REDIS_URL", "redis://127.0.0.1:6379"))
ESRI_API_KEY = os.environ.get('ESRI_API_KEY')

version = .1

d_format = "%Y-%m-%d"
month_step = 60*60*24*30.25
url = constants.url1

observation_count = 13657673

info = Info(url)
start_date, end_date, start_date_seconds, end_date_seconds = info.get_times()
variables, long_names, units, stardard_names, var_types = info.get_variables()

startdt = datetime.datetime.fromtimestamp(start_date_seconds)
enddt = datetime.datetime.fromtimestamp(end_date_seconds)

days = enddt - startdt

obs_day = observation_count/days.days

# Only one so set all_
all_start_seconds = start_date_seconds
all_end_seconds = end_date_seconds

all_start = start_date
all_end = end_date

time_marks = Info.get_time_marks(start_date_seconds, end_date_seconds)

locations_df = pd.read_csv('https://datalocal.pmel.noaa.gov/erddap/tabledap/MOVE1_microcat.csv?site_code%2Clatitude%2Clongitude&distinct()', skiprows=[1])
locations_df = locations_df.mean()
locations_df = locations_df.reset_index().pivot_table(columns='index')

app.layout = html.Div([
    html.Div(id='kick'),
    dcc.Store(id='initial-time-start'),  # time from initial load query string
    dcc.Store(id='initial-time-end'),  # time from initial load query string
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
                    dbc.RadioItems(id='variable',
                        options=[
                            {'label': 'Conductivity', 'value': 'CNDC'},
                            {'label': 'Temperature', 'value': 'TEMP'},
                            {'label': 'Pressure', 'value': 'PRES'},
                            {'label': 'Salinity', 'value': 'PSAL'},
                            
                        ],
                        value='PSAL'
                    )
                ])  
            ]),
            dbc.Card(children=[
                dbc.CardHeader(children=['In the selected time range:']),
                dbc.Row(children=[
                    dbc.Col(width=6, children=[
                        dbc.Card(children=[
                            dbc.CardHeader(children=['Start Date']),
                        ]),
                        dbc.Input(id='start-date', debounce=True, value=all_start)
                    ]),
                    dbc.Col(width=6, children=[
                        dbc.Card(children=[
                            dbc.CardHeader(children=['End Date']),
                        ]),
                        dbc.Input(id='end-date', debounce=True, value=all_end)
                    ])
                ]),
                dbc.Row(children=[
                    dbc.Col(width=12, children=[
                        html.Div(style={'padding-right': '40px', 'padding-left': '40px',
                                        'padding-top': '20px', 'padding-bottom': '45px'}, children=[
                            dcc.RangeSlider(id='time-range-slider',
                                            value=[all_start_seconds,all_end_seconds],
                                            min=all_start_seconds,
                                            max=all_end_seconds,
                                            step=month_step,
                                            marks=time_marks,
                                            updatemode='mouseup',
                                            allowCross=False)
                        ])
                    ])
                ]),
            ]),
        ]),
        dbc.Col(width=8, children=[ddk.Graph(id='location-graph')]),
    ]),

    dbc.Row(children=[
        dbc.Card(
            [
                dbc.CardHeader('Profile Plot'),
                dbc.CardBody(dcc.Loading(ddk.Graph(id='profile-graph')))
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
    [
        Output('location-graph', 'figure'),
    ],
    [
        Input('kick', 'n_clicks')
    ]
)
def update_location_map(kick):
    figure = go.Figure()
    yellow_trace = go.Scattermapbox(lat=locations_df['latitude'].values,
                                lon=locations_df['longitude'].values,
                                hovertext=['MOVE1'],
                                hoverinfo='lat+lon+text',
                                customdata=['MOVE1'],
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
        mapbox_zoom=2,
        mapbox_center={'lat': locations_df['latitude'].values[0], 'lon': locations_df['longitude'].values[0]},
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
        Output('download-body', 'children'),
        Output('download-button', 'disabled')
    ],
    [
        Input('kick', 'n_clicks'),
        Input('variable', 'value'),
        Input('start-date', 'value'),
        Input('end-date', 'value')
    ]
)
def update_profile_plot(kick, in_variable, p_in_start_date, p_in_end_date):
    list_group = html.Div()
    list_group.children = []
    link_group = dbc.ListGroup(horizontal=True)
    link_group.children = []
    list_group.children.append(link_group)
    meta_item = dbc.ListGroupItem('MOVE1 ', href=url, target='_blank')
    link_group.children.append(meta_item)
    if in_variable is None or len(in_variable) == 0:
        return no_update

    if in_variable == 'PSAL':
        cs=px.colors.sequential.Viridis
    else:
        cs=px.colors.sequential.Inferno            
    get_vars = ','.join(variables)
    time_con = '&time>='+p_in_start_date+'&time<='+p_in_end_date
    p_startdt = datetime.datetime.strptime(p_in_start_date, d_format)
    p_enddt = datetime.datetime.strptime(p_in_end_date, d_format)
    days = p_enddt - p_startdt
    estim_obs = days.days*obs_day
    factor = (20000/estim_obs)*100.
    print('The factor is ', factor)
    if factor < 5:
        print('Reducing sample by a factor of ' + str(factor))
        t0 = datetime.datetime.now()
        df = db.get_some(factor)
        df = df.loc[(df['time']>=p_in_start_date) & (df['time']<p_in_end_date)]
        p_title = long_names[in_variable] + ' (sub-sampled: Showing ' + str(df.shape[0]) + ' points of ~' + str(round(estim_obs)) + ') at MOVE1'
        print('Finsihed: ', (datetime.datetime.now() - t0), ' seconds for ', days.days+2, ' days or estimated ', estim_obs, ' observations, with a final sample size of ', df.shape[0])
    else:    
        print('Getting full resolution with ' + str(estim_obs) + ' points expected.')
        t0 = datetime.datetime.now()
        df = db.get_between('MOVE1', p_in_start_date, p_in_end_date)
        p_title = long_names[in_variable] + ' at MOVE1'
        if df.shape[0] > 20000:
            df_actual = df.shape[0]
            df = df.sample(20000)
            p_title = long_names[in_variable] + ' (sub-sampled: Showing ' + str(df.shape[0]) + ' points of ~' + str(round(df_actual)) + ') at MOVE1' 
        print('Finsihed: ', (datetime.datetime.now() - t0), ' seconds for ', days.days+2, ' days or estimated ', estim_obs, ' observations, with a final sample size of ', df.shape[0])
    figure = px.scatter(df, x='time', y='PRES', color=in_variable, color_continuous_scale=cs, title=p_title)
    figure.update_yaxes(autorange='reversed')
    p_url = url
    p_url = p_url + '.csv?' + get_vars + time_con + '&site_code="MOVE1"'
    item = dbc.ListGroupItem('.html', href=p_url.replace('.csv', '.htmlTable'), target='_blank')
    link_group.children.append(item)
    item = dbc.ListGroupItem('.csv', href=p_url.replace('.htmlTable', '.csv'), target='_blank')
    link_group.children.append(item)
    item = dbc.ListGroupItem('.nc', href=p_url.replace('.csv', '.ncCF'), target='_blank')
    link_group.children.append(item)
    return [figure, list_group, False]


@app.callback(
    [
        Output('time-range-slider', 'value'),
        Output('start-date', 'value'),
        Output('end-date', 'value')
    ],
    [
        Input('time-range-slider', 'value'),
        Input('start-date', 'value'),
        Input('end-date', 'value'),
        Input('initial-time-start', 'data'),
        Input('initial-time-end', 'data')
    ], prevent_initial_call=True
)
def set_date_range_from_slider(slide_values, in_start_date, in_end_date, initial_start, initial_end):
    ctx = callback_context
    trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]
    if trigger_id == 'initial-time-start' or trigger_id == 'initial-time-end':
        start_output = initial_start
        end_output = initial_end
        try:
            in_start_date_obj = datetime.datetime.strptime(initial_start, d_format)
            start_seconds = in_start_date_obj.timestamp()
        except:
            start_seconds = all_start_seconds

        try:
            in_end_date_obj = datetime.datetime.strptime(initial_end, d_format)
            end_seconds = in_end_date_obj.timestamp()
        except:
            end_seconds = all_end_seconds

    else:
        if slide_values is None:
            raise exceptions.PreventUpdate

        range_min = all_start_seconds
        range_max = all_end_seconds

        ctx = callback_context
        trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]

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
        elif trigger_id == 'time-range-slider':
            in_start_date_obj = datetime.datetime.fromtimestamp(slide_values[0])
            start_output = in_start_date_obj.strftime(d_format)
            in_end_date_obj = datetime.datetime.fromtimestamp(slide_values[1])
            end_output = in_end_date_obj.strftime(d_format)

    return [[start_seconds, end_seconds],
            start_output,
            end_output
            ]


if __name__ == '__main__':
    app.run_server(debug=True)
