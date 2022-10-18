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


def _cut_bin(x,
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

    if not isinstance(bins, Iterable):
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

    if precision is not None:
        if closed != "left":
            bins = np.ceil(10 ** precision * bins, out=bins) / 10 ** precision
        else:
            bins = np.floor(10 ** precision * bins, out=bins) / 10 ** precision

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


def bin_test(y_true, y_pred, x,
             bins=10,
             cut_method="quantile",
             closed="right",
             precision=3,
             fillna=np.nan,
             n_jobs=5):
    # calculate bad label count and cumsum it
    def _bin_helper(bin_):
        bin_y_true = y_true.groupby(bin_).sum()
        total_y_true = y_true.sum(axis=0)

        bin_y_pred = y_pred.groupby(bin_).sum()
        total_y_pred = y_pred.sum(axis=0)

        bad_rate = bin_y_true / total_y_true
        good_rate = (~y_true).groupby(bin_).sum() / (~y_true).sum()
        ks = (bad_rate.cumsum() - good_rate.cumsum()).abs()

        expect_rate = bin_y_pred / total_y_pred
        psi = (bad_rate - expect_rate) \
              * np.log(bad_rate / expect_rate, where=bad_rate / expect_rate != 0)
        return {
            "true_positive": bin_y_true,
            "pred_positive": bin_y_pred,
            "bad_rate": bad_rate,
            "good_rate": good_rate,
            "ks": ks,
            "psi": psi
        }

    bins = _cut_bin(x,
                    bins=bins,
                    cut_method=cut_method,
                    closed=closed,
                    precision=precision,
                    fillna=fillna)
    if x.ndim == 1:
        return _bin_helper(bins)
    elif x.ndim == 2:
        d_future = {}
        with ThreadPoolExecutor(max_workers=n_jobs) as executor:
            for i, b in enumerate(bins):
                d_future[
                    executor.submit(
                        _bin_helper,
                        bin_=b)
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
