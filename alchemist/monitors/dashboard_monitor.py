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

@ray.remote
class DashboardMonitor(BaseMonitor):
    def __init__(self, host='0.0.0.0', port=8050):
        self.host = host
        self.port = port
        
        # Data storage
        self.latencies = {
            'market_data_to_zmq': deque(maxlen=1000),
            'strategy_update_to_order': deque(maxlen=1000),
            'gateway_order_roundtrip': deque(maxlen=1000),
            'strategy_order_roundtrip': deque(maxlen=1000),
            'strategy_order_ack': deque(maxlen=1000),
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
        self.app.layout = html.Div([
            html.H1("Trading System Monitor"),
            dcc.Interval(id='interval-component', interval=1000, n_intervals=0),
            html.Div([
                html.Div([
                    html.H3("Latencies (ms)"),
                    dcc.Graph(id='latency-graph'),
                ], style={'width': '48%', 'display': 'inline-block'}),
                html.Div([
                    html.H3("Slippage"),
                    dcc.Graph(id='slippage-graph'),
                ], style={'width': '48%', 'display': 'inline-block'}),
            ])
        ])

        @self.app.callback(
            [Output('latency-graph', 'figure'),
             Output('slippage-graph', 'figure')],
            [Input('interval-component', 'n_intervals')]
        )
        def update_graphs(n):
            # Create Latency Figure
            latency_traces = []
            for name, data in self.latencies.items():
                if data:
                    df = pd.DataFrame(data)
                    latency_traces.append(go.Scatter(
                        x=df['ts'],
                        y=df['value'] * 1000, # Convert to ms
                        mode='lines+markers',
                        name=name
                    ))
            
            latency_fig = go.Figure(data=latency_traces)
            latency_fig.update_layout(title="System Latencies", yaxis_title="Latency (ms)")

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
            slippage_fig.update_layout(title="Order Slippage", yaxis_title="Slippage")

            return latency_fig, slippage_fig

    def log_latency(self, metric_name, value, ts=None):
        if ts is None:
            ts = datetime.now()
        if metric_name in self.latencies:
            self.latencies[metric_name].append({'ts': ts, 'value': value})

    def log_slippage(self, value, ts=None):
        if ts is None:
            ts = datetime.now()
        self.slippages.append({'ts': ts, 'value': value})

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
