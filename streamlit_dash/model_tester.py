import os
import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

# Page configuration
st.set_page_config(page_title="Late Delivery Risk Predictor", layout="wide", page_icon="📦")

st.title("📦 Late Delivery Risk Prediction - Model Tester")
st.markdown("Test the trained model with custom order inputs to predict late delivery risk.")

# Initialize Spark session
@st.cache_resource
def get_spark():
    try:
        spark = SparkSession.builder \
            .appName("model-tester") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    except Exception as e:
        st.error(f"Failed to initialize Spark: {e}")
        return None

# Load the saved model
@st.cache_resource
def load_model(_spark):
    model_dir = os.environ.get("MODEL_DIR", "/src/models")
    model_path = os.path.join(model_dir, "late_delivery_pipeline_safe")
    
    try:
        if not os.path.exists(model_path):
            st.error(f"Model not found at {model_path}")
            return None
        
        model = PipelineModel.load(model_path)
        st.success(f"✅ Model loaded from {model_path}")
        return model
    except Exception as e:
        st.error(f"Error loading model: {e}")
        return None

spark = get_spark()
if spark is None:
    st.stop()

model = load_model(spark)
if model is None:
    st.stop()

# Create input form
st.sidebar.header("Order Details")
st.sidebar.markdown("Fill in the order information below:")

# Categorical inputs
with st.sidebar.expander("📍 Location & Shipping", expanded=True):
    shipping_mode = st.selectbox(
        "Shipping Mode",
        ["Standard Class", "Second Class", "First Class", "Same Day"],
        help="Delivery speed tier"
    )
    
    market = st.selectbox(
        "Market",
        ["US", "LATAM", "Europe", "Africa", "Pacific Asia", "USCA"],
        help="Geographic market region"
    )
    
    order_region = st.selectbox(
        "Order Region",
        ["West", "East", "Central", "South"],
        help="Regional shipping zone"
    )
    
    customer_segment = st.selectbox(
        "Customer Segment",
        ["Consumer", "Corporate", "Home Office"],
        help="Type of customer"
    )

with st.sidebar.expander("🏷️ Product Information", expanded=True):
    department_id = st.selectbox(
        "Department ID",
        list(range(1, 8)),
        help="Product department identifier"
    )
    
    category_id = st.selectbox(
        "Category ID",
        list(range(1, 25)),
        help="Product category identifier"
    )
    
    product_category_id = st.selectbox(
        "Product Category ID",
        list(range(1, 75)),
        help="Specific product category"
    )

with st.sidebar.expander("💰 Order Details", expanded=True):
    days_scheduled = st.slider(
        "Days for Shipment (Scheduled)",
        min_value=0, max_value=10, value=3,
        help="Promised delivery time in days"
    )
    
    quantity = st.number_input(
        "Order Item Quantity",
        min_value=1, max_value=100, value=2,
        help="Number of items in the order"
    )
    
    price = st.number_input(
        "Order Item Product Price ($)",
        min_value=0.0, max_value=10000.0, value=100.0, step=0.01,
        help="Price per item"
    )
    
    discount = st.number_input(
        "Order Item Discount ($)",
        min_value=0.0, max_value=1000.0, value=0.0, step=0.01,
        help="Discount amount"
    )

with st.sidebar.expander("📅 Order Timing", expanded=True):
    order_hour = st.slider(
        "Order Hour (0-23)",
        min_value=0, max_value=23, value=14,
        help="Hour when order was placed"
    )
    
    order_dow = st.selectbox(
        "Order Day of Week",
        {1: "Sunday", 2: "Monday", 3: "Tuesday", 4: "Wednesday", 
         5: "Thursday", 6: "Friday", 7: "Saturday"},
        format_func=lambda x: {1: "Sunday", 2: "Monday", 3: "Tuesday", 
                               4: "Wednesday", 5: "Thursday", 6: "Friday", 
                               7: "Saturday"}[x],
        help="Day of week when order was placed"
    )
    
    order_month = st.slider(
        "Order Month",
        min_value=1, max_value=12, value=6,
        help="Month when order was placed"
    )

with st.sidebar.expander("🌍 Coordinates (Optional)", expanded=False):
    latitude = st.number_input(
        "Latitude",
        min_value=-90.0, max_value=90.0, value=40.0, step=0.1,
        help="Delivery location latitude"
    )
    
    longitude = st.number_input(
        "Longitude",
        min_value=-180.0, max_value=180.0, value=-100.0, step=0.1,
        help="Delivery location longitude"
    )

# Calculate discount ratio
discount_ratio = discount / price if price > 0 else 0.0

# Main area - Display inputs and prediction
col1, col2 = st.columns([1, 1])

with col1:
    st.subheader("📋 Order Summary")
    
    summary_data = {
        "**Shipping & Location**": {
            "Shipping Mode": shipping_mode,
            "Market": market,
            "Region": order_region,
            "Customer Segment": customer_segment,
        },
        "**Product**": {
            "Department ID": department_id,
            "Category ID": category_id,
            "Product Category ID": product_category_id,
        },
        "**Order Details**": {
            "Scheduled Days": days_scheduled,
            "Quantity": quantity,
            "Price": f"${price:.2f}",
            "Discount": f"${discount:.2f}",
            "Discount Ratio": f"{discount_ratio:.2%}",
        },
        "**Timing**": {
            "Hour": f"{order_hour}:00",
            "Day of Week": {1: "Sun", 2: "Mon", 3: "Tue", 4: "Wed", 
                           5: "Thu", 6: "Fri", 7: "Sat"}[order_dow],
            "Month": order_month,
        }
    }
    
    for section, values in summary_data.items():
        st.markdown(f"**{section}**")
        for k, v in values.items():
            st.text(f"  {k}: {v}")
        st.markdown("")

with col2:
    st.subheader("🎯 Prediction Results")
    
    if st.button("🔮 Predict Late Delivery Risk", type="primary", use_container_width=True):
        try:
            # Create input DataFrame
            input_data = {
                "label": 0,  # Dummy label (not used in prediction)
                "Shipping Mode": shipping_mode,
                "Market": market,
                "Order Region": order_region,
                "Customer Segment": customer_segment,
                "Department Id": department_id,
                "Category Id": category_id,
                "Product Category Id": product_category_id,
                "Days for shipment (scheduled)": days_scheduled,
                "Order Item Quantity": quantity,
                "Order Item Product Price": price,
                "Order Item Discount": discount,
                "discount_ratio": discount_ratio,
                "Latitude": latitude,
                "Longitude": longitude,
                "order_hour": order_hour,
                "order_dow": order_dow,
                "order_month": order_month,
            }
            
            # Convert to Spark DataFrame
            input_df = spark.createDataFrame([input_data])
            
            # Make prediction
            with st.spinner("Making prediction..."):
                prediction_df = model.transform(input_df)
                result = prediction_df.select("prediction", "probability").collect()[0]
                
                prediction = int(result["prediction"])
                probability = result["probability"].toArray()
                prob_late = probability[1]  # Probability of late delivery (class 1)
                prob_ontime = probability[0]  # Probability of on-time (class 0)
            
            # Display results
            st.markdown("---")
            
            if prediction == 1:
                st.error("⚠️ **HIGH RISK** - Delivery likely to be late")
                st.metric("Late Delivery Probability", f"{prob_late:.1%}", 
                         delta=f"{prob_late - 0.5:.1%}" if prob_late > 0.5 else None)
            else:
                st.success("✅ **LOW RISK** - Delivery likely on time")
                st.metric("On-Time Delivery Probability", f"{prob_ontime:.1%}",
                         delta=f"{prob_ontime - 0.5:.1%}" if prob_ontime > 0.5 else None)
            
            st.markdown("---")
            st.markdown("**Probability Breakdown:**")
            prob_col1, prob_col2 = st.columns(2)
            with prob_col1:
                st.metric("On-Time", f"{prob_ontime:.2%}")
            with prob_col2:
                st.metric("Late", f"{prob_late:.2%}")
            
            # Visualization
            st.markdown("**Risk Distribution:**")
            prob_df = pd.DataFrame({
                'Status': ['On-Time', 'Late'],
                'Probability': [prob_ontime, prob_late]
            })
            st.bar_chart(prob_df.set_index('Status'))
            
            # Recommendation
            st.markdown("---")
            st.markdown("**💡 Recommendation:**")
            if prob_late > 0.7:
                st.warning("Consider upgrading shipping mode or contacting customer about potential delay.")
            elif prob_late > 0.5:
                st.info("Monitor this order closely. Consider proactive communication.")
            else:
                st.success("Order should proceed normally. Standard monitoring recommended.")
                
        except Exception as e:
            st.error(f"Prediction failed: {e}")
            import traceback
            st.code(traceback.format_exc())

# Footer
st.markdown("---")
st.caption("🤖 Powered by PySpark ML | Model: late_delivery_pipeline_safe")

# Optional: Show model info
with st.expander("ℹ️ Model Information"):
    st.markdown("""
    **Model Type:** Random Forest Classifier with preprocessing pipeline
    
    **Features Used:**
    - Categorical: Shipping Mode, Market, Order Region, Customer Segment, Department/Category IDs
    - Numeric: Scheduled days, Quantity, Price, Discount, Discount ratio, Coordinates, Order timing
    
    **Preprocessing:**
    - Median imputation for numeric features
    - String indexing and one-hot encoding for categoricals
    - Standard scaling
    - Class weight balancing
    
    **Training:** Cross-validated with 80/20 train-test split
    """)
    
    # Load and display metrics if available
    metrics_path = os.path.join(os.environ.get("MODEL_DIR", "/src/models"), "metrics.json")
    if os.path.exists(metrics_path):
        import json
        with open(metrics_path, 'r') as f:
            metrics = json.load(f)
        st.json(metrics)
