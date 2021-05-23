try:
    import pandas as pd
except ImportError:

    class PandasInterface:
        DataFrame = object
        Series = object

    pd = PandasInterface
