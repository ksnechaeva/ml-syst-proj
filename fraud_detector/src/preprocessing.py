import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def load_train_data():
    logger.info('Loading training data...')

    # Import Train dataset
    train = pd.read_csv('./train_data/train.csv')
    logger.info('Raw train data imported. Shape: %s', train.shape)

    # Add some features
    train = add_features(train)
    logger.info('Train data processed. Shape: %s', train.shape)

    return train


def haversine(lat1, lon1, lat2, lon2):
    rad = 6371
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return rad * c


def add_features(df):
    """Build project-specific behavioural, geo and calendar features.

    The transformation is deliberately shared by offline training and online
    scoring.  It does not fit mutable encoders, so retrying it is idempotent.
    """
    df_ = df.copy()
    df_['distance_km'] = haversine(df_['lat'], df_['lon'],
                                   df_['merchant_lat'], df_['merchant_lon'])
    # Compatibility with the seminar's bundled bootstrap model. New models use
    # both this stable legacy column and the project-specific features below.
    df_['haversine'] = df_['distance_km']
    df_['amount_log1p'] = np.log1p(pd.to_numeric(df_['amount'], errors='coerce').clip(lower=0))
    df_['amount_is_round'] = (pd.to_numeric(df_['amount'], errors='coerce').fillna(0) % 10 == 0).astype(int)
    df_['far_from_home'] = (df_['distance_km'] > 100).astype(int)
    df_["transaction_time"] = pd.to_datetime(df_["transaction_time"])
    df_["year"] = df_["transaction_time"].dt.year
    df_["month"] = df_["transaction_time"].dt.month
    df_["day"] = df_["transaction_time"].dt.day
    df_["hour"] = df_["transaction_time"].dt.hour
    df_["minute"] = df_["transaction_time"].dt.minute
    df_["weekend"] = (df_["transaction_time"].dt.dayofweek > 4).astype(int)
    df_["night"] = df_["hour"].isin([0, 1, 2, 3, 4, 5]).astype(int)
    df_["high_amount"] = (pd.to_numeric(df_["amount"], errors="coerce").fillna(0) >= 500).astype(int)
    return df_.drop(columns=["transaction_time", "name_1", "name_2", "street"])


# Main preprocessing function
def run_preproc(input_df):
    # Add features
    output_df = add_features(input_df)
    logger.info('Added new features. Output shape: %s', output_df.shape)

    # Return resulting dataset
    return output_df
