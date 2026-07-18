"""Print QUIK accounts without starting Cerebro or placing orders."""
from backtrader_quik import QKStore, configure_console_logging


def main():
    configure_console_logging()
    store = QKStore()
    try:
        if not store.provider.accounts:
            print('QUIK returned no trade accounts')
            return
        for account in store.provider.accounts:
            print(account)
    finally:
        store.stop()


if __name__ == '__main__':
    main()
