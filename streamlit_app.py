import streamlit as st
import snowflake.connector
from anthropic import Anthropic
import pandas as pd

# Load secrets from Streamlit's built-in secrets manager
snowflake_config = st.secrets["snowflake"]
anthropic_api_key = st.secrets["anthropic"]["api_key"]

# Snowflake connection
def create_snowflake_connection():
    try:
        return snowflake.connector.connect(
            account=snowflake_config["account"],
            user=snowflake_config["user"],
            password=snowflake_config["password"],
            warehouse=snowflake_config["warehouse"],
            database="ACQ4",  # Explicitly specify the ACQ4 database
            schema="PUBLIC"  # Explicitly specify the PUBLIC schema
        )
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")
        return None


# Function to query Snowflake
def query_snowflake(query):
    try:
        conn = create_snowflake_connection()
        if conn is None:
            return None
        cur = conn.cursor()
        cur.execute(query)
        results = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()
        conn.close()
        return pd.DataFrame(results, columns=columns)
    except Exception as e:
        st.error(f"Error querying Snowflake: {e}")
        return None


# Anthropic setup
try:
    anthropic_client = Anthropic(api_key=anthropic_api_key)
except Exception as e:
    st.error(f"Error initializing Anthropic client: {e}")


# Function to get Anthropic response
def get_anthropic_response(prompt):
    try:
        response = anthropic_client.completions.create(
            model="claude-v1",
            prompt=f"\n\nHuman: {prompt}\n\nAssistant:",
            max_tokens_to_sample=500,
            stop_sequences=["\n\nHuman:"]
        )
        return response.completion.strip()
    except Exception as e:
        st.error(f"Error getting response from Anthropic: {e}")
        return None


# Streamlit app
st.title('Data Analysis with Snowflake and Claude')

# Query to fetch data from the ACQ4 database
snowflake_query = """
SELECT * FROM ACQ4.PUBLIC.ACMEQ4DISPLAY
"""

# Load data from Snowflake
queried_data = query_snowflake(snowflake_query)

# Average Conversion Rate Section
st.subheader("Average Conversion Rate")

if queried_data is not None and not queried_data.empty and "CONVERSION_RATE" in queried_data.columns:
    average = int(queried_data["CONVERSION_RATE"].mean())
else:
    average = 0  # Fallback if the table or column is empty

hifives_val = st.slider(
    "Average Conversion Rate",
    min_value=0,
    max_value=10,
    value=average,
)

# Campaigns Data Section
st.subheader("Campaigns Data")

if queried_data is not None and not queried_data.empty:
    # Display a bar chart (ensure queried_data has the required columns)
    if "WEEK_NUMBER" in queried_data.columns and "WEEKLY_BUDGET_SPENT" in queried_data.columns:
        st.subheader("Budget Spent by Week")
        st.bar_chart(data=queried_data, x="WEEK_NUMBER", y="WEEKLY_BUDGET_SPENT")
    else:
        st.write("The required columns for the bar chart are missing in the data.")

    # Display the entire dataset
    st.subheader("All Data Set")
    st.dataframe(queried_data, use_container_width=True)
else:
    st.error("Failed to load data or data is empty.")

# Claude Ask Question Section
st.subheader("Analyze Data with Claude")

# Initialize session state for data
if 'data' not in st.session_state:
    st.session_state['data'] = queried_data

question = st.text_input("Ask a question about the data:")

if st.button('Get Answer'):
    if question and st.session_state['data'] is not None:
        # Convert the entire dataset to a string for Anthropic
        full_data = st.session_state['data'].to_string(index=False)

        # Prepare the prompt with the entire dataset
        prompt = f"The following table shows the entire dataset:\n\n{full_data}\n\nQuestion: {question}\n\nAnswer:"

        # Get answer from Anthropic
        answer = get_anthropic_response(prompt)
        if answer:
            st.write("Answer:")
            st.write(answer)
        else:
            st.error("Failed to get a response. Please try again.")
    else:
        st.error("Please load the data first and enter a valid question.")