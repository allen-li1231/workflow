import pandas as pd
import numpy as np
import datetime as dt
from typing import Iterable
from sklearn.metrics import (roc_curve,
                             roc_auc_score,
                             precision_recall_fscore_support,
                             balanced_accuracy_score)
from sklearn.utils.multiclass import type_of_target
from .metrics import bin_stat


def binary_classification_report(y_true,
                                 y_score,
                                 pos_label=1,
                                 threshold=None,
                                 bin_x=None,
                                 bins=10,
                                 cut_method="quantile",
                                 ascending=True,
                                 sample_weight=None,
                                 precision=5
                                 ):
    """
    function that calculates model performance stats

    y_true : ndarray of shape (n_samples,)
        True binary labels. If labels are not either {-1, 1} or {0, 1}, then
        pos_label should be explicitly given.

    y_score : ndarray of shape (n_samples,)
        Target scores, can either be probability estimates of the positive
        class, confidence values, or non-thresholded measure of decisions
        (as returned by "decision_function" on some classifiers).

    pos_label : int or str, default=None
        The label of the positive class.
        When ``pos_label=None``, if `y_true` is in {-1, 1} or {0, 1},
        ``pos_label`` is set to 1, otherwise an error will be raised.

    threshold : float, default=None
        The threshold to differentiate y_score, values in threshold larger than
        threshold will be considered positive.
    
    bin_x : 1 or 2-dim-array of shape (n_samples,)
        Values to cut bins on

    bins : int, sequence of scalars, or IntervalIndex
        The criteria to bin by.
        * int : Defines the number of equal-width bins in the range of x.
            The range of x is extended by .1% on each side to include
            the minimum and maximum values of x.
        * sequence of scalars : Defines the bin edges allowing
            for non-uniform width.
            No extension of the range of x is done.
        * IntervalIndex : Defines the exact bins to be used.
            Note that IntervalIndex for bins must be non-overlapping.

    cut_method : str, enumerate from ('quantile', 'percentile',)

    ascending : boolean, default=True
        Sort ascending vs. descending. When the index is a MultiIndex
        the sort direction can be controlled for each level individually.

    sample_weight : array-like of shape (n_samples,), default=None
        Sample weights.
    
    precision : int or None, default=5
        The precision to round the results

    Statistic Formulas:
        bad_num = y_true.groupby(bin_).sum()
        bin_num = y_true.groupby(bin_).count()

        bad_num.sort_index(ascending=ascending)
        bin_num.sort_index(ascending=ascending)
        good_num = bin_num - bad_num

        bad_rate = bad_num / bin_num
        good_rate = 1. - bad_rate

        odds = good_rate / bad_rate

        bin_rate = bin_num / bin_num.sum()
        cum_bin_num = bin_num.cumsum()
        cum_bin_bad_num = bad_num.cumsum()
        cum_bin_bad_rate = cum_bin_bad_num / cum_bin_num
        global_bad_rate = bad_num / bad_num.sum()
        cum_global_bad_rate = global_bad_rate.cumsum()

        cum_bin_good_num = good_num.cumsum()
        cum_bin_good_rate = cum_bin_good_num / cum_bin_num
        global_good_rate = good_num / good_num.sum()
        cum_global_good_rate = global_good_rate.cumsum()
        # ks = (cum_global_bad_rate - cum_global_good_rate).abs()
        lift = cum_bin_bad_rate / cum_bin_bad_rate.iloc[-1]
    """

    # TODO: variable PSI to be added to report
    type_y_true, type_y_pred = type_of_target(y_true), type_of_target(y_score)
    if type_y_true != 'binary' or type_y_pred != 'continuous':
        raise ValueError("y_true must be binary labels and y_pred must be continuous values (0-1)")

    fpr, tpr, ths = roc_curve(y_true=y_true, y_score=y_score,
                              pos_label=pos_label, sample_weight=sample_weight)
    auc = (tpr * np.diff(fpr, prepend=0.)).sum()
    ks = abs(fpr - tpr).max()

    d_model_stats = {
        # "fpr": fpr.round(precision).tolist(),
        # "tpr": tpr.round(precision).tolist(),
        # "threshold": ths.round(precision).tolist(),
        "auc": np.round(auc, precision),
        "ks": np.round(ks, precision),
    }

    if threshold is not None:
        y_pred = y_score > threshold

        (pos_p, neg_p), (pos_r, neg_r), (pos_f1, neg_f1), (pos_s, neg_s) = \
            precision_recall_fscore_support(y_true=y_true, y_pred=y_pred,
                                            pos_label=pos_label, sample_weight=sample_weight)
        balanced_acc = balanced_accuracy_score(y_true=y_true, y_pred=y_pred,
                                            sample_weight=sample_weight)
        if bin_x is None:
            binned_stat = None
        else:
            binned_stat = bin_stat(bin_x=bin_x, y_true=y_true,
                                   y_pred=y_pred, bins=bins,
                                   cut_method=cut_method, ascending=ascending,
                                   precision=precision)
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
                               cut_method=cut_method, ascending=ascending,
                               precision=precision)

    d_model_stats.update(predict_stats)
    d_model_stats["bin_stats"] = binned_stat

    return d_model_stats


def binary_classification_report_by_date(
        y_true,
        y_score,
        label=1,
        threshold=None,
        bin_x=None,
        bins=10,
        cut_method="quantile",
        ascending=True,
        date_x: str = None,
        date_cutoffs: list = None,
        datetime_format: str = '%Y-%m-%d',
        strftime: str = '%Y%m%d',
        date_sep: str = ' ~ ',
        bin_stat_cols: list = None,
        sample_weight=None,
        precision=5
        ):
    """
    function that calculates model performance stats

    y_true : ndarray of shape (n_samples,)
        True binary labels. If labels are not either {-1, 1} or {0, 1}, then
        pos_label should be explicitly given.

    y_score : ndarray of shape (n_samples,)
        Target scores, can either be probability estimates of the positive
        class, confidence values, or non-thresholded measure of decisions
        (as returned by "decision_function" on some classifiers).

    pos_label : int or str, default=None
        The label of the positive class.
        When ``pos_label=None``, if `y_true` is in {-1, 1} or {0, 1},
        ``pos_label`` is set to 1, otherwise an error will be raised.

    threshold : float, default=None
        The threshold to differentiate y_score, values in threshold larger than
        threshold will be considered positive.
    
    bin_x : 1 or 2-dim-array of shape (n_samples,)
        Values to cut bins on

    bins : int, sequence of scalars, or IntervalIndex
        The criteria to bin by.
        * int : Defines the number of equal-width bins in the range of x.
            The range of x is extended by .1% on each side to include
            the minimum and maximum values of x.
        * sequence of scalars : Defines the bin edges allowing
            for non-uniform width.
            No extension of the range of x is done.
        * IntervalIndex : Defines the exact bins to be used.
            Note that IntervalIndex for bins must be non-overlapping.

    cut_method : str, enumerate from ('quantile', 'percentile',)
        Quantile-based or percentile-based binning method.

    ascending : boolean, default=True
        Sort ascending vs. descending. When the index is a MultiIndex
        the sort direction can be controlled for each level individually.

    date_x : array-like of shape (n_samples,)
        Date values for date_cutoffs to cut on.
        Will ignore date cut if not given.

    date_cutoffs : IntervalIndex, default=None
        The criteria to cut date by: the exact bins to be used.
        Note that IntervalIndex for bins must be non-overlapping.
        Will ignore date cut if not given

    datetime_format : str, default='%Y-%m-%d'
        The correct date format of date_x array, we need to convert it
        to datetime type to derive the correct date labels

    strftime : str, default='%Y%m%d'
        The sliced date range's format in index/column
        which is returned along with stat data.

    date_sep : str, default=' ~ '
        The seperation sign between date ranges.

    bin_stat_cols : array-like of stats names, default=None
        if specified, will only return specified bin stats
        Supported bin statistic formulas:
            bad_num = y_true.groupby(bin_).sum()
            bin_num = y_true.groupby(bin_).count()

            bad_num.sort_index(ascending=ascending)
            bin_num.sort_index(ascending=ascending)
            good_num = bin_num - bad_num

            bad_rate = bad_num / bin_num
            good_rate = 1. - bad_rate

            odds = good_rate / bad_rate

            bin_rate = bin_num / bin_num.sum()
            cum_bin_num = bin_num.cumsum()
            cum_bin_bad_num = bad_num.cumsum()
            cum_bin_bad_rate = cum_bin_bad_num / cum_bin_num
            global_bad_rate = bad_num / bad_num.sum()
            cum_global_bad_rate = global_bad_rate.cumsum()

            cum_bin_good_num = good_num.cumsum()
            cum_bin_good_rate = cum_bin_good_num / cum_bin_num
            global_good_rate = good_num / good_num.sum()
            cum_global_good_rate = global_good_rate.cumsum()

            lift = cum_bin_bad_rate / cum_bin_bad_rate.iloc[-1]
            total_bad_rate = cum_bin_bad_num.iloc[-1] / cum_bin_num.iloc[-1]

    sample_weight : array-like of shape (n_samples,), default=None
        Sample weights.
    
    precision : int or None, default=5
        The precision to round the results

    Usage examples:

    >>> df_stats, df_bin_stats = binary_classification_report_by_date(
            df_y["target"],
            df_y["y_pred"],
            bin_x=df_y["score"],
            bins=bins,
            ascending=True,
            date_x=df_y["loan_day"],
            date_cutoffs=["2023-09-15", "2023-10-20"],
            stat_cols=None, 
        )
    >>> df_stats
                            auc       ks
    20230609 ~ 20230914  0.55072  0.13010
    20230915 ~ 20231019  0.62877  0.18950
    20231020 ~ 20231021  0.59387  0.17466

    >>> df_bin_stats
                                            bin_num  bad_num  good_num  bin_rate  
    date                bin                                                    
    20230609 ~ 20230914 (-inf, 569.0]      3499     2339      1160  0.204895   ...
                        (569.0, 600.0]     3416     2091      1325  0.200035   ...
                        (600.0, 627.0]     3349     1975      1374  0.196112   ...
                        (627.0, 665.0]     3417     1811      1606  0.200094   ...
                        (665.0, inf]       3396     1407      1989  0.198864   ...
    20230915 ~ 20231019 (-inf, 606.0]      2295     1293      1002  0.200910   ...
                        (606.0, 647.0]     2305     1158      1147  0.201786   ...
                        (647.0, 686.0]     2300      970      1330  0.201348   ...
                        (686.0, 735.0]     2250      795      1455  0.196971   ...
                        (735.0, inf]       2273      606      1667  0.198985   ...
    20231020 ~ 20231021 (-inf, 646.0]       140       57        83  0.202899   ...
                        (646.0, 692.0]      140       57        83  0.202899   ...
                        (692.0, 731.0]      135       38        97  0.195652   ...
                        (731.0, 774.0]      138       38       100  0.200000   ...
                        (774.0, inf]        137       30       107  0.198551   ...
    """

    # senario 1: date separation info not fully given
    if date_x is None or date_cutoffs is None or len(date_cutoffs) == 0:
        d_model_stats = binary_classification_report(
            y_true=y_true, y_score=y_score, pos_label=label, threshold=threshold,
            bin_x=bin_x, bins=bins, cut_method=cut_method, ascending=ascending,
            sample_weight=sample_weight, precision=precision)

        if bin_stat_cols:
            d_model_stats["bin_stats"] = d_model_stats["bin_stats"][bin_stat_cols]

        return d_model_stats

    # convert date_x to datetime type, so date labels can be correctly generated
    if not pd.api.types.is_datetime64_any_dtype(date_x):
        srs_date = pd.to_datetime(date_x)
    else:
        srs_date = date_x

    min_date, max_date = srs_date.min(), srs_date.max()
    date_cutoffs = sorted(date_cutoffs)
    if min_date not in date_cutoffs:
        date_cutoffs.insert(0, min_date)
    if max_date not in date_cutoffs:
        date_cutoffs.append(max_date)

    # make last day inclusive
    date_cutoffs[-1] += dt.timedelta(days=1)

    # generate each report for each date range
    lst_date_ranges = []
    lst_stats = []
    lst_bin_stats = []
    last_dt = None
    for i, date in enumerate(date_cutoffs):
        # convert cutoff date to datetime type
        date = date if isinstance(date, dt.datetime) else dt.datetime.strptime(date, datetime_format)
        if i == 0:
            last_dt = date
            continue

        srs_date_filter = (srs_date >= last_dt) & (srs_date < date)
        if srs_date_filter.sum() == 0:
            continue

        d_model_stats = binary_classification_report(
            y_true=y_true[srs_date_filter], y_score=y_score[srs_date_filter],
            pos_label=label, threshold=threshold,
            bin_x=bin_x if bin_x is None else np.extract(srs_date_filter, bin_x),
            bins=bins, cut_method=cut_method, ascending=ascending,
            sample_weight=sample_weight, precision=precision)

        df_bin_stats = d_model_stats["bin_stats"]

        if bin_stat_cols:
            df_bin_stats = df_bin_stats[bin_stat_cols]

        str_dt_yesterday = date - dt.timedelta(days=1)
        str_date_range = f"{last_dt.strftime(strftime)}{date_sep}{str_dt_yesterday.strftime(strftime)}"
        lst_date_ranges.append(str_date_range)

        if isinstance(bins, Iterable):
            df_bin_stats.columns = pd.MultiIndex.from_tuples([
                (str_date_range, col)
                for col in df_bin_stats.columns],
                names=["date", "stat"])
            df_bin_stats.index.names = ["bin"]
        else:
            df_bin_stats["date"] = str_date_range
            df_bin_stats.set_index(["date", df_bin_stats.index], inplace=True)
            df_bin_stats.index.names = ["date", "bin"]

        lst_bin_stats.append(df_bin_stats)
        del d_model_stats["bin_stats"]
        lst_stats.append(d_model_stats)

        last_dt = date

    df_bin_stats = pd.concat(lst_bin_stats,
                             axis=1 if isinstance(bins, Iterable) else 0,
                             copy=False)

    df_stats = pd.DataFrame(lst_stats, copy=False, index=lst_date_ranges)

    if isinstance(precision, int):
        df_stats, df_bin_stats = df_stats.round(precision), df_bin_stats.round(precision)

    return df_stats, df_bin_stats


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

    result['TOTAL_BAD_RATE'] = round(result['CUM_GLOBAL_BAD_RATE'].iloc[-1], 4)

    result['KS'] = round(abs(result['CUM_POP_BAD_RATE'] - result['CUM_GLOBAL_GOOD_RATE']), 4)
    result['LIFT'] = round(abs(result['CUM_POP_BAD_RATE'] / result['CUM_POP_BAD_RATE'].iloc[-1]), 4)

    result = result[['GOOD_NUM', 'BAD_NUM', 'TOTAL', 'POP', 'ODDS', 'TOTAL_BAD_RATE',
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
                                            pos_label=label)
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
