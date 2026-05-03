# src/ml/model.py
import polars as pl
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.metrics import (
    mean_squared_error, r2_score,
    accuracy_score, precision_score, recall_score,
    confusion_matrix
)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
import mlflow
import mlflow.sklearn
import matplotlib.pyplot as plt
import seaborn as sns
import tempfile
import logging
from deltalake import DeltaTable

def train_models(gold_feat_path: str):
    logging.info("Loading feature table for ML")
    
    # Установка эксперимента MLflow
    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment("FlightDelayPrediction")
    
    # Загрузка feature table (Polars -> pandas)
    df = pl.read_delta(gold_feat_path).to_pandas()
    logging.info(f"Feature table shape: {df.shape}")
    
    # Кодирование категориальных признаков (сезон исключён)
    le_airline = LabelEncoder()
    le_origin = LabelEncoder()
    le_dest = LabelEncoder()
    
    df["Airline_enc"] = le_airline.fit_transform(df["Airline"])
    df["Origin_enc"] = le_origin.fit_transform(df["Origin"])
    df["Dest_enc"] = le_dest.fit_transform(df["Dest"])
    
    # Признаки (без season_enc, так как он константен)
    feature_cols = ["DepartureDelay", "hour", "Distance", "Airline_enc", "Origin_enc", "Dest_enc"]
    
    X = df[feature_cols]
    
    # Регрессия
    y_reg = df["ArrivalDelay"]
    # Классификация: задержка > 15 минут
    y_cls = (df["ArrivalDelay"] > 15).astype(int)
    
    # Разделение
    X_train_reg, X_test_reg, y_train_reg, y_test_reg = train_test_split(
        X, y_reg, test_size=0.2, random_state=42
    )
    X_train_cls, X_test_cls, y_train_cls, y_test_cls = train_test_split(
        X, y_cls, test_size=0.2, random_state=42
    )
    
    # Версия gold-таблицы
    dt = DeltaTable(gold_feat_path)
    gold_version = dt.version()
    
    # ------------------------------
    # 1. Линейная регрессия
    # ------------------------------
    with mlflow.start_run(run_name="LinearRegression"):
        mlflow.log_param("gold_table_version", gold_version)
        mlflow.log_param("model_type", "LinearRegression")
        mlflow.log_param("feature_columns", feature_cols)
        
        lr = LinearRegression()
        lr.fit(X_train_reg, y_train_reg)
        y_pred = lr.predict(X_test_reg)
        
        mse = mean_squared_error(y_test_reg, y_pred)
        r2 = r2_score(y_test_reg, y_pred)
        mlflow.log_metric("mse", mse)
        mlflow.log_metric("r2", r2)
        
        for name, coef in zip(feature_cols, lr.coef_):
            mlflow.log_param(f"coef_{name}", coef)
        mlflow.log_param("intercept", lr.intercept_)
        mlflow.sklearn.log_model(lr, "model")
        logging.info(f"LinearRegression: MSE={mse:.3f}, R2={r2:.3f}")
    
    # ------------------------------
    # 2. Random Forest Regressor
    # ------------------------------
    with mlflow.start_run(run_name="RandomForestRegressor"):
        mlflow.log_param("gold_table_version", gold_version)
        mlflow.log_param("model_type", "RandomForestRegressor")
        mlflow.log_param("n_estimators", 100)
        mlflow.log_param("feature_columns", feature_cols)
        
        rf_reg = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
        rf_reg.fit(X_train_reg, y_train_reg)
        y_pred = rf_reg.predict(X_test_reg)
        
        mse = mean_squared_error(y_test_reg, y_pred)
        r2 = r2_score(y_test_reg, y_pred)
        mlflow.log_metric("mse", mse)
        mlflow.log_metric("r2", r2)
        
        for name, imp in zip(feature_cols, rf_reg.feature_importances_):
            mlflow.log_metric(f"importance_{name}", imp)
        
        # График actual vs predicted
        fig, ax = plt.subplots(figsize=(6,5))
        ax.scatter(y_test_reg, y_pred, alpha=0.3)
        ax.plot([y_test_reg.min(), y_test_reg.max()],
                [y_test_reg.min(), y_test_reg.max()], 'r--')
        ax.set_xlabel("Actual Arrival Delay")
        ax.set_ylabel("Predicted Arrival Delay")
        ax.set_title(f"Random Forest Regressor (R2={r2:.3f})")
        with tempfile.NamedTemporaryFile(suffix=".png") as tmp:
            plt.savefig(tmp.name)
            mlflow.log_artifact(tmp.name, artifact_path="plots")
        plt.close()
        
        mlflow.sklearn.log_model(rf_reg, "model")
        logging.info(f"RandomForestRegressor: MSE={mse:.3f}, R2={r2:.3f}")
    
    # ------------------------------
    # 3. Логистическая регрессия
    # ------------------------------
    with mlflow.start_run(run_name="LogisticRegression"):
        mlflow.log_param("gold_table_version", gold_version)
        mlflow.log_param("model_type", "LogisticRegression")
        mlflow.log_param("threshold_minutes", 15)
        mlflow.log_param("feature_columns", feature_cols)
        
        lr_clf = LogisticRegression(max_iter=1000, random_state=42)
        lr_clf.fit(X_train_cls, y_train_cls)
        y_pred = lr_clf.predict(X_test_cls)
        
        acc = accuracy_score(y_test_cls, y_pred)
        prec = precision_score(y_test_cls, y_pred, zero_division=0)
        rec = recall_score(y_test_cls, y_pred)
        mlflow.log_metric("accuracy", acc)
        mlflow.log_metric("precision", prec)
        mlflow.log_metric("recall", rec)
        
        # Confusion matrix
        fig, ax = plt.subplots(figsize=(5,4))
        sns.heatmap(confusion_matrix(y_test_cls, y_pred), annot=True, fmt='d', ax=ax, cmap='Blues')
        ax.set_title("Logistic Regression Confusion Matrix")
        with tempfile.NamedTemporaryFile(suffix=".png") as tmp:
            plt.savefig(tmp.name)
            mlflow.log_artifact(tmp.name, artifact_path="plots")
        plt.close()
        
        mlflow.sklearn.log_model(lr_clf, "model")
        logging.info(f"LogisticRegression: accuracy={acc:.3f}, precision={prec:.3f}, recall={rec:.3f}")
    
    # ------------------------------
    # 4. Random Forest Classifier
    # ------------------------------
    with mlflow.start_run(run_name="RandomForestClassifier"):
        mlflow.log_param("gold_table_version", gold_version)
        mlflow.log_param("model_type", "RandomForestClassifier")
        mlflow.log_param("n_estimators", 100)
        mlflow.log_param("threshold_minutes", 15)
        mlflow.log_param("feature_columns", feature_cols)
        
        rf_clf = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
        rf_clf.fit(X_train_cls, y_train_cls)
        y_pred = rf_clf.predict(X_test_cls)
        
        acc = accuracy_score(y_test_cls, y_pred)
        prec = precision_score(y_test_cls, y_pred, zero_division=0)
        rec = recall_score(y_test_cls, y_pred)
        mlflow.log_metric("accuracy", acc)
        mlflow.log_metric("precision", prec)
        mlflow.log_metric("recall", rec)
        
        for name, imp in zip(feature_cols, rf_clf.feature_importances_):
            mlflow.log_metric(f"importance_{name}", imp)
        
        # Комбинированный график: confusion matrix + feature importance
        fig, axes = plt.subplots(1, 2, figsize=(12,5))
        cm = confusion_matrix(y_test_cls, y_pred)
        sns.heatmap(cm, annot=True, fmt='d', ax=axes[0], cmap='Greens')
        axes[0].set_title("Confusion Matrix")
        
        importances = rf_clf.feature_importances_
        indices = np.argsort(importances)[::-1]
        axes[1].barh(range(len(indices)), importances[indices])
        axes[1].set_yticks(range(len(indices)))
        axes[1].set_yticklabels([feature_cols[i] for i in indices])
        axes[1].set_title("Feature Importance")
        axes[1].set_xlabel("Importance")
        plt.tight_layout()
        with tempfile.NamedTemporaryFile(suffix=".png") as tmp:
            plt.savefig(tmp.name)
            mlflow.log_artifact(tmp.name, artifact_path="plots")
        plt.close()
        
        mlflow.sklearn.log_model(rf_clf, "model")
        logging.info(f"RandomForestClassifier: accuracy={acc:.3f}, precision={prec:.3f}, recall={rec:.3f}")
    
    logging.info("All models trained and logged")