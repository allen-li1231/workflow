import pandas as pd
import numpy as np
import datetime as dt
from sklearn.metrics import (roc_curve,
                             roc_auc_score,
                             precision_recall_fscore_support,
                             balanced_accuracy_score)
from sklearn.utils.multiclass import type_of_target
from .metrics import bin_stat


def binary_classification_report(y_true,
                                 y_score,
                                 label=1,
                                 threshold=None,
                                 bin_x=None,
                                 bins=10,
                                 cut_method="quantile",
                                 sample_weight=None,
                                 precision=5
                                 ):
    # TODO: variable PSI to be added to report
    type_y_true, type_y_pred = type_of_target(y_true), type_of_target(y_score)
    if type_y_true != 'binary' or type_y_pred != 'continuous':
        raise ValueError("y_true must be binary labels and y_pred must be continuous values (0-1)")

    fpr, tpr, ths = roc_curve(y_true=y_true, y_score=y_score,
                              pos_label=label, sample_weight=sample_weight)
    auc = (tpr * np.diff(fpr, prepend=0.)).sum()
    ks = abs(fpr - tpr).max()

    score_stats = {
        "roc": {
            "fpr": fpr.round(precision).tolist(),
            "tpr": tpr.round(precision).tolist(),
            "threshold": ths.round(precision).tolist()
        },
        "auc": np.round(auc, precision),
        "ks": np.round(ks, precision),
    }

    if threshold is not None:
        y_pred = y_score > threshold

        (pos_p, neg_p), (pos_r, neg_r), (pos_f1, neg_f1), (pos_s, neg_s) = \
            precision_recall_fscore_support(y_true=y_true, y_pred=y_pred,
                                            pos_label=label, sample_weight=sample_weight)
        balanced_acc = balanced_accuracy_score(y_true=y_true, y_pred=y_pred,
                                            sample_weight=sample_weight)
        if bin_x is None:
            binned_stat = None
        else:
            binned_stat = bin_stat(bin_x=bin_x, y_true=y_true,
                                   y_pred=y_pred, bins=bins,
                                   cut_method=cut_method, precision=precision)
        predict_stats = {
            "balanced_acc": np.round(balanced_acc, precision), 
            "precision": [np.round(pos_p, precision), np.round(neg_p, precision)],
            "recall": [np.round(pos_r, precision), np.round(neg_r, precision)],
            "f1": [np.round(pos_f1, precision), np.round(neg_f1, precision)],
            "support": [np.round(pos_s, precision), np.round(neg_s, precision)],
        }
    else:
        predict_stats = {}
        binned_stat = bin_stat(bin_x=bin_x, y_true=y_true,
                               y_pred=None, bins=bins,
                               cut_method=cut_method, precision=precision)

    score_stats.update(predict_stats)
    score_stats["bin_stats"] = binned_stat

    return score_stats


def performance_table(data, target, py_cut, ascending=True):
    mdata = data[[target, py_cut]]
    result = mdata.groupby([py_cut], as_index=False).agg({target:['count', 'sum']})
    result.columns = ['SCORE_CUT', 'TOTAL', 'BAD_NUM']
    result.set_index('SCORE_CUT', drop=True, inplace=True)
    result.sort_index(ascending=ascending, inplace=True)

    result['GOOD_NUM'] = result['TOTAL'] - result['BAD_NUM']
    result['BAD_RATE'] = round(result['BAD_NUM'] / result['TOTAL'], 4)
    result['GOOD_RATE'] = round(result['GOOD_NUM'] / result['TOTAL'], 4)
    result['ODDS'] = round((1-result['BAD_RATE']) / result['BAD_RATE'], 3)
    result['POP'] = round(result['TOTAL'] / result['TOTAL'].sum(), 4)
    result['CUM_POP_BAD_RATE'] = round(result['BAD_NUM'].cumsum() / result['TOTAL'].cumsum(), 4)
    result['GLOBAL_BAD_RATE'] = round(result['BAD_NUM'] / result['BAD_NUM'].sum(), 4)
    result['CUM_GLOBAL_BAD_RATE'] = round(result['GLOBAL_BAD_RATE'].cumsum(), 4)
    result['GLOBAL_GOOD_RATE'] = round(result['GOOD_NUM'] / result['GOOD_NUM'].sum(), 4)
    result['CUM_GLOBAL_GOOD_RATE'] = round(result['GLOBAL_GOOD_RATE'].cumsum(), 4)
    result['KS'] = round(abs(result['CUM_POP_BAD_RATE'] - result['CUM_GLOBAL_GOOD_RATE']), 4)
    result['LIFT'] = round(abs(result['CUM_POP_BAD_RATE'] / result['CUM_POP_BAD_RATE'].iloc[-1]), 4)

    result = result[['GOOD_NUM', 'BAD_NUM', 'TOTAL', 'POP', 'ODDS', 
                    'GOOD_RATE', 'BAD_RATE', 'GLOBAL_BAD_RATE', 'CUM_POP_BAD_RATE', 'CUM_GLOBAL_BAD_RATE',
                    'GLOBAL_GOOD_RATE', 'CUM_GLOBAL_GOOD_RATE', 'KS', 'LIFT']]
    # result = result.loc[:, ['TOTAL', 'BAD_NUM', 'POP', 'BAD_RATE']]
    return result


def get_performance_by_date(data: pd.DataFrame,
                            target_col,
                            bins_col,
                            date_col: str = None,
                            date_cutoffs: list = None,
                            stat_cols: list = None,
                            y_pred_col: str = None,
                            datetime_format: str = '%Y-%m-%d',
                            strftime: str = '%Y%m%d',
                            ascending=True
                            ):

    if date_col is None or date_cutoffs is None or len(date_cutoffs) == 0:
        df_performance = performance_table(data, target_col, bins_col, ascending)
        if y_pred_col is not None:
            auc = roc_auc_score(data.loc[:, target_col], data.loc[:, y_pred_col])

        if stat_cols:
            df_performance = df_performance[stat_cols]

        if y_pred_col is not None:
            return df_performance, auc

        return df_performance

    if not pd.api.types.is_datetime64_any_dtype(data[date_col]):
        srs_date = pd.to_datetime(data[date_col])
    else:
        srs_date = data[date_col]

    min_date, max_date = srs_date.min(), srs_date.max()
    date_cutoffs = sorted(date_cutoffs)
    if min_date not in date_cutoffs:
        date_cutoffs.insert(0, min_date)
    if max_date not in date_cutoffs:
        date_cutoffs.append(max_date)

    date_cutoffs[-1] += dt.timedelta(days=1)

    lst_stats = []
    lst_df = []
    last_dt = None
    for i, date in enumerate(date_cutoffs):
        date = date if isinstance(date, dt.datetime) else dt.datetime.strptime(date, datetime_format)
        if i == 0:
            last_dt = date
            continue

        srs_date_filter = (srs_date >= last_dt) & (srs_date < date)
        if srs_date_filter.sum() == 0:
            continue

        if y_pred_col is not None:
            lst_stats.append(
                roc_auc_score(data.loc[srs_date_filter, target_col], data.loc[srs_date_filter, y_pred_col])
            )

        df_performance = performance_table(data[srs_date_filter], target_col, bins_col, ascending)
        if stat_cols:
            df_performance = df_performance[stat_cols]
        
        str_dt_yesterday = date - dt.timedelta(days=1)
        str_date_range = f"{last_dt.strftime(strftime)} ~ {str_dt_yesterday.strftime(strftime)}"

        df_performance.columns = pd.MultiIndex.from_tuples([(str_date_range, col)
                                                             for col in df_performance.columns],
                                                             names=["date", "stat"])
        lst_df.append(df_performance)
        last_dt = date

    if y_pred_col is None:
        return pd.concat(lst_df, axis=1, copy=False)

    return pd.concat(lst_df, axis=1, copy=False), lst_stats


def model_report(y_true, y_score, x, threshold=None, label=None, sample_weight=None):
    type_y_true, type_y_pred = type_of_target(y_true), type_of_target(y_score)
    if type_y_true == 'binary' and type_y_pred == 'continuous':
        return binary_classification_report(y_true=y_true, y_score=y_score, bin_x=x,
                                            threshold=threshold, sample_weight=sample_weight,
                                            label=label)
    raise NotImplementedError("type of target not implemented yet")


if __name__ == "__main__":
    # tests
    df = pd.read_excel("model_compare.xlsx")
    df["dummy_prob"] = df["prob"].sample(len(df)).values
    df["x1"] = np.random.randint(10, 100, df.shape[0])
    df["x2"] = np.random.randint(-100, 0, df.shape[0])

    fpr, tpr, threshold = roc_curve(df["dummy_prob"] > 0.9, df["prob"])
    all_stats = binary_classification_report(y_true=df["dummy_prob"] > 0.6, y_score=df["prob"],
                                             threshold=0.6, bin_x=df[["x1", "x2"]])
