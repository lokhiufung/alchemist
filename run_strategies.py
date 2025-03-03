import os
from dotenv import load_dotenv

load_dotenv(".env")

import argparse
import typing

import yaml

from alchemist.management_service import TradeManagementService
from alchemist.account import Account
from alchemist.products.base_product import BaseProduct
from alchemist.data_card import DataCard
from alchemist.common.helpers import import_class  # Assuming this function dynamically imports classes


def load_yaml(file_path):
    """Load configuration from YAML file."""
    with open(file_path, "r") as f:
        return yaml.safe_load(f)


def load_products(config) -> typing.List[BaseProduct]:
    """Load product configurations."""
    products = []
    for product in config:
        Product = import_class(product.pop("type"))  # Dynamically import the class
        products.append(Product(**product))  # Pass only the required attributes
    return products


def load_accounts(config) -> typing.List[Account]:
    """Load account configurations."""
    accounts = [Account(**account) for account in config]    
    return accounts


def load_data_cards(config, products) -> typing.List[DataCard]:
    """Load data card configurations."""
    data_cards = [DataCard(**data_card) for data_card in config]
    for data_card in data_cards:
        data_card.product = next(product for product in products if product.name == data_card.product)
    return data_cards


def load_gateways(config, products, accounts):
    """Dynamically load and initialize gateway instances from config."""
    gateways = {}
    for gateway_name, gateway_config in config.items():
        gateway_class = import_class(gateway_config["type"])  # Dynamically import the class
        gateways[gateway_name] = gateway_class.remote(
            subscriptions=gateway_config.get("subscriptions", []),
            products=products,
            zmq_send_port=gateway_config["zmq_send_port"],
            zmq_recv_ports=gateway_config["zmq_recv_ports"],
            accounts=accounts
        )
    return gateways


def load_strategies(config, products, data_cards):
    """Dynamically load and initialize strategy instances from config."""
    strategies = {}
    for strategy_name, strategy_config in config.items():
        strategy_class = import_class(strategy_config["type"])  # Dynamically import the class
        strategies[strategy_name] = strategy_class.remote(
            name=strategy_name,
            zmq_send_port=strategy_config["zmq_send_port"],
            zmq_recv_ports=strategy_config["zmq_recv_ports"],
            products=products,
            data_cards=data_cards,
            monitor_actor=strategy_config.get("monitor_actor", None)
        )
    return strategies


def create_management_service():
    """Create and return the trade management service."""
    file_path = os.getenv("STRATEGY_CONFIG_FILE_PATH")

    config = load_yaml(file_path)

    products = load_products(config["products"])
    accounts = load_accounts(config["accounts"])
    data_cards = load_data_cards(config["data_cards"], products)

    gateways = load_gateways(config["gateways"], products, accounts)
    strategies = load_strategies(config["strategies"], products, data_cards)

    management_service = TradeManagementService.bind(gateways, strategies)
    return management_service


management_service = create_management_service()
