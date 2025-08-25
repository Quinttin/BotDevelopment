"""Simple training script for demonstration purposes.

Loads data from ``historical_seed.csv`` and trains both a scikit-learn
SGDClassifier and a small Keras neural network. The trained models are
saved to ``sklearn_trading_model.pkl`` and ``keras_trading_model.keras``
respectively.
"""

import pandas as pd
from sklearn.linear_model import SGDClassifier
from sklearn.model_selection import train_test_split
from tensorflow import keras
import joblib


def main() -> None:
    df = pd.read_csv("historical_seed.csv")
    X = df.drop("target", axis=1)
    y = df["target"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    clf = SGDClassifier(max_iter=1000, tol=1e-3)
    clf.fit(X_train, y_train)
    joblib.dump(clf, "sklearn_trading_model.pkl")

    model = keras.Sequential(
        [
            keras.layers.Dense(16, activation="relu", input_shape=(X_train.shape[1],)),
            keras.layers.Dense(1, activation="sigmoid"),
        ]
    )
    model.compile(optimizer="adam", loss="binary_crossentropy", metrics=["accuracy"])
    model.fit(X_train, y_train, epochs=5, verbose=0)
    model.save("keras_trading_model.keras")
    print("Models saved.")


if __name__ == "__main__":
    main()
