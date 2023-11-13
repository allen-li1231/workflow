import pandas as pd
import numpy as np
from sklearn.metrics import (roc_curve,
                             precision_recall_fscore_support,
                             balanced_accuracy_score)
from sklearn.utils.multiclass import type_of_target
from .metrics import bin_test


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
            binned_stat = bin_test(bin_x=bin_x, y_true=y_true,
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
        binned_stat = bin_test(bin_x=bin_x, y_true=y_true,
                               y_pred=None, bins=bins,
                               cut_method=cut_method, precision=precision)

    score_stats.update(predict_stats)
    score_stats["bin_stats"] = binned_stat

    return score_stats


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
