from statsmodels.tsa.stattools import grangercausalitytests
import numpy as np
import pandas as pd


def granger_causality_matrix(
    data, variables, test="ssr_chi2test", verbose=False, maxlag=1
):

    df = pd.DataFrame(
        np.zeros((len(variables), len(variables))),
        columns=variables,
        index=variables,
    )
    for c in df.columns:
        for r in df.index:
            test_result = grangercausalitytests(
                data[[r, c]], maxlag=maxlag, verbose=False
            )
            p_values = [
                round(test_result[i + 1][0][test][1], 4) for i in range(maxlag)
            ]
            if verbose:
                print(f"Y = {r}, X = {c}, P Values = {p_values}")
            min_p_value = np.min(p_values)
            df.loc[r, c] = min_p_value
    df.columns = [var + "_x" for var in variables]
    df.index = [var + "_y" for var in variables]
    return df
