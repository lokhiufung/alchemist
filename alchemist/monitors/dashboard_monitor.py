import threading
import time
from datetime import datetime
from collections import deque
import pandas as pd
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import ray

from alchemist.monitors.base_monitor import BaseMonitor


def convert_float_to_datetime(ts):
    return datetime.fromtimestamp(int(ts))


@ray.remote
class DashboardMonitor(BaseMonitor):
    def __init__(self, host='0.0.0.0', port=8050):
        self.host = host
        self.port = port
        
        # Data storage
        self.latencies = {
            # 'market_data_to_zmq': deque(maxlen=1000),
            'strategy_update_to_order': deque(maxlen=1000),
            'strategy_order_fill_latency': deque(maxlen=1000),
            'strategy_order_roundtrip': deque(maxlen=1000),
            # 'strategy_order_ack': deque(maxlen=1000),
        }
        self.slippages = deque(maxlen=1000)
        
        self.app = dash.Dash(__name__)
        self.setup_layout()
        
        # Start Dash server in a separate thread
        self.server_thread = threading.Thread(target=self.run_server)
        self.server_thread.daemon = True
        self.server_thread.start()

    def run_server(self):
        self.app.run_server(host=self.host, port=self.port, debug=False, use_reloader=False)

    def setup_layout(self):
        graphs = []
        # Create a graph for each latency metric
        for key in self.latencies:
            graphs.append(html.Div([
                html.H3(f"{key.replace('_', ' ').title()} (ms)"),
                dcc.Graph(id=f'latency-{key}'),
            ], style={'width': '48%', 'display': 'inline-block', 'padding': '10px'}))
            
        # Add slippage graph
        graphs.append(html.Div([
            html.H3("Slippage"),
            dcc.Graph(id='slippage-graph'),
        ], style={'width': '48%', 'display': 'inline-block', 'padding': '10px'}))

        self.app.layout = html.Div([
            html.H1("Trading System Monitor"),
            dcc.Interval(id='interval-component', interval=1000, n_intervals=0),
            html.Div(graphs, style={'display': 'flex', 'flex-wrap': 'wrap'})
        ])

        # Define outputs dynamically
        outputs = [Output(f'latency-{key}', 'figure') for key in self.latencies]
        outputs.append(Output('slippage-graph', 'figure'))

        @self.app.callback(
            outputs,
            [Input('interval-component', 'n_intervals')]
        )
        def update_graphs(n):
            figures = []
            
            # Create figures for each latency metric
            for key in self.latencies:
                data = self.latencies[key]
                traces = []
                if data:
                    df = pd.DataFrame(data)
                    traces.append(go.Scatter(
                        x=df['ts'],
                        y=df['value'] * 1000, # Convert to ms
                        mode='lines+markers',
                        name=key
                    ))
                
                fig = go.Figure(data=traces)
                fig.update_layout(
                    title=f"{key.replace('_', ' ').title()}", 
                    yaxis_title="Latency (ms)",
                    margin=dict(l=40, r=40, t=40, b=40),
                    height=300
                )
                figures.append(fig)

            # Create Slippage Figure
            slippage_traces = []
            if self.slippages:
                df = pd.DataFrame(self.slippages)
                slippage_traces.append(go.Scatter(
                    x=df['ts'],
                    y=df['value'],
                    mode='markers',
                    name='Slippage'
                ))
            
            slippage_fig = go.Figure(data=slippage_traces)
            slippage_fig.update_layout(
                title="Order Slippage", 
                yaxis_title="Slippage",
                margin=dict(l=40, r=40, t=40, b=40),
                height=300
            )
            figures.append(slippage_fig)

            return figures

    def log_latency(self, metric_name, value, ts=None):
        if ts is None:
            ts = datetime.now()
        if metric_name in self.latencies:
            self.latencies[metric_name].append({'ts': convert_float_to_datetime(ts), 'value': value})

    def log_slippage(self, value, ts=None):
        if ts is None:
            ts = datetime.now()
        self.slippages.append({'ts': convert_float_to_datetime(ts), 'value': value})

    # Implement abstract methods from BaseMonitor
    def log_signal(self, strategy_name, ts, signal_name, value):
        pass 

    def log_trade(self, strategy_name, ts, oid, pdt, filled_price, filled_size, side):
        pass

    def log_order(self, strategy_name, ts, oid, order_type, time_in_force, pdt, status, size, price, side):
        pass

    def log_position(self, strategy_name, ts, pdt, size, side, avg_price, realized_pnl, unrealized_pnl):
        pass

    def log_portfolio(self, strategy_name, ts, balance, margin_used, total_value):
        pass
