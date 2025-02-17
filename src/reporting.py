import locale
from tabulate import tabulate
from config import Config

class ContractReporter:
    """Handles report generation and formatting."""
    
    def __init__(self, config: Config) -> None:
        self.cofnig = config
        locale.setlocale(locale.LC_ALL, config.locale_setting)
        
    def format_currency(self, amount: float) -> str:
        """Format any amount as currency string."""
        try:
            return locale.currency(amount, grouping=True)
        except locale.Error:
            return f"${amount:,.2f}"
        
    def generate_savings_report(self, total_value: float, obligated_amount: float) -> str:
        """Generate formatted savings report."""
        value_saved = total_value - obligated_amount
        
        data = [
            ["Total Contract Value", self.format_currency(total_value)],
            ["Total Obligated Amount", self.format_currency(obligated_amount)],
            ["Potential Savings", self.format_currency(value_saved)]
        ]
        
        table = tabulate(
            data, headers=["Metric", "Amount"], tablefmt="fancy_grid"
        )
        
        title = "\033[1mDOGE Savings\033[0m"
        width = len(table.splitlines()[0])
        centered_title = title.center(width)
        
        return (
            "\n" * 9 +
            centered_title + "\n" +
            "-" * width + "\n" +
            table
        )