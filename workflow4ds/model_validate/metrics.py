import pandas as pd
import numpy as np
from typing import Iterable
from sklearn.metrics import (confusion_matrix,
                             roc_auc_score,
                             roc_curve,
                             precision_recall_fscore_support,
                             balanced_accuracy_score,
                             mutual_info_score,
                             adjusted_mutual_info_score,
                             mean_absolute_error,
                             classification_report)

from concurrent.futures import ThreadPoolExecutor, as_completed


def PSI(y_true, y_pred):
    quotient = y_pred / y_true
    psi = (y_pred - y_true) \
          * np.log(quotient, where=quotient != 0)
    return psi


def cut_bin(x,
            bins=10,
            cut_method="quantile",
            closed="right",
            precision=None,
            fillna=np.nan,
            retbin=False
            ):

    if not isinstance(bins, Iterable) and (not np.isreal(bins) or bins < 2):
        raise ValueError("bins should be iterable or integer no less than 2")

    if cut_method not in ('quantile', "percentile"):
        raise ValueError(
            f"cut method should be either 'quantile' or 'percentile, got '{cut_method}'"
        )

    x = np.asarray(x)
    if x.ndim > 2:
        raise ValueError("currently not support x with axis larger than 2")

    b_bins_is_cut = isinstance(bins, Iterable)
    if not b_bins_is_cut:
        # rewrite pandas cut/qcut methods to support inf boundaries
        if cut_method == 'quantile':
            q = np.linspace(0, 1, bins + 1)
            bins = np.nanquantile(x, q, axis=0)
        elif cut_method == 'percentile':
            mn, mx = np.nanmin(x, axis=0), np.nanmax(x, axis=0)
            if np.any((np.isinf(mn), np.isinf(mx))):
                # GH 24314
                raise ValueError(
                    "cannot specify integer `bins` when x contains infinity"
                )
            bins = np.linspace(mn, mx, bins + 1, axis=0)

        bins[0, ...], bins[-1, ...] = -np.inf, np.inf

    side = "right" if closed != "right" else "left"
    if bins.ndim == 1:
        ids = bins.searchsorted(x, side=side)
    else:
        ids = [b.searchsorted(col, side=side) for b, col in zip(bins.T, x.T)]
        ids = np.vstack(ids).T

    # edge case: bin breaks equal to the first value of corresponding bin
    # are arranged to the first bin, which should be in the second bin
    # (the index of bin will be subsequently subtracted by one).
    if side == "left":
        ids[x == bins[0]] = 1
    else:
        ids[x == bins[-1]] = len(bins) - 1

    # put back na values
    na_mask = pd.isna(x) | (ids == len(bins)) | (ids == 0)
    if na_mask.any():
        np.putmask(ids, na_mask, 0)

    # push na's respective positional index to -1
    # from Pandas doc: "If allow_fill=True and fill_value is not None,
    # indices specified by -1 are regarded as NA. If Index doesn’t hold NA, raise ValueError."
    ids -= 1

    if not b_bins_is_cut and precision is not None:
        if closed != "left":
            bins = np.ceil((10 ** precision) * bins, out=bins) / (10 ** precision)
        else:
            bins = np.floor((10 ** precision) * bins, out=bins) / (10 ** precision)

    if bins.ndim == 1:
        labels = pd.IntervalIndex.from_breaks(np.unique(bins), closed=closed)
        ret = labels.take(ids, fill_value=fillna)
        # ret = pd.Categorical(ret, categories=labels, ordered=True)
    else:
        labels = [pd.IntervalIndex.from_breaks(b, closed=closed) for b in bins.T]
        ret = [lb.take(i, fill_value=fillna) for lb, i in zip(labels, ids.T)]
        # ret = [pd.Categorical(r, categories=lb, ordered=True) for r, lb in zip(ret, labels)]

    if retbin:
        return ret, bins

    return ret


def bin_stat(bin_x,
             y_true,
             y_pred=None,
             bins=10,
             cut_method="quantile",
             ascending=True,
             closed="right",
             precision=3,
             fillna=np.nan,
             n_jobs=5):

    # calculate bad label count and cumsum it
    def _bin_stat_helper(bin_, ascending=True):
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

        d_stat = {"bin_num": bin_num, "bad_num": bad_num, "good_num": good_num,
                  "bin_rate": bin_rate, "bad_rate": bad_rate, "good_rate": good_rate,
                  "odds": odds,
                  "global_bad_rate": global_bad_rate, "global_good_rate": global_good_rate,
                  "cum_bin_bad_rate": cum_bin_bad_rate, "cum_bin_good_rate": cum_bin_good_rate,
                  "cum_global_bad_rate": cum_global_bad_rate, "cum_global_good_rate": cum_global_good_rate,
                  # "ks": ks, 
                  "lift": lift}

        if isinstance(y_pred, Iterable):
            bin_y_pred = y_pred.groupby(bin_).sum()
            total_y_pred = y_pred.groupby(bin_).count()
            expect_rate = bin_y_pred / total_y_pred
            psi = PSI(expect_rate, bad_rate)
            d_stat["psi"] = psi

        df_stat = pd.DataFrame(d_stat, copy=False)
        df_stat["total_bad_rate"] = cum_bin_bad_rate.iloc[-1]
        return df_stat

    if not isinstance(y_true, pd.Series):
        y_true = pd.Series(y_true, copy=True, name="y_true")

    if not y_pred is None and not isinstance(y_pred, pd.Series):
        y_pred = pd.Series(y_pred, copy=True, name="y_pred")

    bins = cut_bin(bin_x,
                   bins=bins,
                   cut_method=cut_method,
                   closed=closed,
                   precision=precision,
                   fillna=fillna)
    if bin_x.ndim == 1:
        return _bin_stat_helper(bins, ascending=ascending)
    elif bin_x.ndim == 2:
        d_future = {}
        with ThreadPoolExecutor(max_workers=n_jobs) as executor:
            for i, b in enumerate(bins):
                d_future[
                    executor.submit(
                        _bin_stat_helper,
                        bin_=b, ascending=ascending)
                ] = i

            # won't work only if returned value is None
            lst_result = [None] * len(d_future)
            for future in as_completed(d_future):
                lst_result[d_future[future]] = future.result()

        return lst_result
    else:
        raise IndexError("x dimension should not larger than 2")


def auc_test(y_true, y_pred, label=None, **auc_kwargs):
    if label is not None:
        y_true = (y_true == label).astype(int)

    return roc_auc_score(y_true=y_true, y_score=y_pred, **auc_kwargs)


def roc_test(y_true, y_pred, label=None, **auc_kwargs):
    if label is not None:
        y_true = (y_true == label)

    return roc_curve(y_true=y_true, y_score=y_pred, **auc_kwargs)


def nunique(arr, axis=0):
    return arr.shape[axis] \
           - (np.diff(np.sort(arr, axis=axis), axis=axis) == 0).sum(axis=axis)
