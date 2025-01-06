import os 
from google.cloud import bigquery
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from collections import Counter
import re

# Get the data via API from BigQuery
@st.cache_data
def get_data():
    # set the service account key file
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './orbital-station-6a5ab-a81b4ea97b16.json'
    # configure the client
    client = bigquery.Client()
    # build the query to consult the data
    sql_query = """
    WITH 
    frases AS (
    SELECT 
        user_pseudo_id,
        MAX(IF(event_params.key IN ("term"), event_params.value.string_value, NULL)) AS utm_term,
        MAX(IF(event_params.key IN ("frase"), event_params.value.string_value, NULL)) AS frase
    FROM 
        `orbital-station-6a5ab.analytics_308291533.events_*`,
        UNNEST(event_params) AS event_params 
    WHERE PARSE_DATE('%Y%m%d', event_date) >= "2024-12-12"
    GROUP BY 
    ALL
    ),
    eventos AS (
    SELECT 
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        FORMAT_TIMESTAMP('%H:%M:%S', TIMESTAMP_MICROS(event_timestamp)) AS event_timestamp,
        event_name,
        user_pseudo_id,
    FROM 
        `orbital-station-6a5ab.analytics_308291533.events_*`
    WHERE PARSE_DATE('%Y%m%d', event_date) >= "2024-12-12"
    GROUP BY 
        ALL
    )
    SELECT 
    eventos.*,
    frases.utm_term,
    frases.frase
    FROM eventos
    LEFT JOIN frases ON frases.user_pseudo_id = eventos.user_pseudo_id
    """
    # execute the consult and convert in a dataframe
    query_job = client.query(sql_query)  # execute the consult
    df_bq = query_job.to_dataframe()    # onvert into a dataframe
    # Return the results
    return df_bq

# Function to check the password
def check_password():
    password = st.text_input("Digite a senha para acessar o dashboard:", type="password")
    if password == st.secrets["credentials"]["password"]:
        return True
    elif password != "":
        st.error("Senha incorreta")
        return False
    return False

# Try to access the application with the given password
if check_password():

    # get the data
    data = get_data()

    # Delete the registers with 'user_pseudo_id' filled as 'unknown' registros onde `user_pseudo_id` é "unknown"
    data = data[data["user_pseudo_id"] != "unknown"]

    # Show a sample of the data
    st.write("### Dados Carregados")
    st.write("Registros com `user_pseudo_id = 'unknown'` foram eliminados.")
    st.dataframe(data[['event_date','event_timestamp','event_name','utm_term','frase']], height=300)  # Enable the scroll with fixed height

    # Create the cards with the number of clients in each step
    st.subheader("Indicadores de Etapas do Funil")

    etapas = {
        "Etapa - 0 - Iniciar": "Iniciar Forms",
        "Etapa - 1 - name": "Preencher Nome",
        "Etapa - 2 - email": "Preencher E-mail",
        "Etapa - 3 - phone": "Preencher Tel.",
        "envio_leads_leadster": "Envio lead",
    }

    # Create the numbers of each step
    valores = [
        data[data["event_name"] == evento]["user_pseudo_id"].nunique()
        for evento in etapas.keys()
    ]

    # Show the cards side by side
    colunas = st.columns(len(etapas))  # Create a column for each card
    for i, coluna in enumerate(colunas):
        with coluna:
            st.metric(label=etapas[list(etapas.keys())[i]], value=valores[i])

    # Filter by `event_name`
    st.subheader("Filtrar por Event Name")
    event_names = data["event_name"].unique()
    selected_event = st.selectbox("Selecione um evento para análise:", ["Todos"] + list(event_names))

    # Apply the filter
    if selected_event != "Todos":
        filtered_data = data[data["event_name"] == selected_event]
    else:
        filtered_data = data

    st.write(f"### Dados para o evento: {selected_event}")
    st.dataframe(filtered_data[['event_date','event_timestamp','event_name','utm_term','frase']], height=300)  # Enable scroll for the table

    # Create the wordcloud
    st.subheader("Nuvem de Palavras")

    # Select unique values
    filtered_data1 = filtered_data[['user_pseudo_id','frase','utm_term']].drop_duplicates() 
    # Workcloud of the filtered data
    all_words = " ".join(filtered_data1["frase"].astype(str))
    all_words_term = " ".join(filtered_data1["utm_term"].astype(str))

    # Remove words with less then 4 digits
    words_filtered = re.findall(r'\b\w{4,}\b', all_words)
    words_filtered_term = re.findall(r'\b\w{2,}\b', all_words_term)

    # Create the wordcloud
    wordcloud_all = WordCloud(width=800, height=400, background_color="white").generate(" ".join(words_filtered))

    st.write("Nuvem de Palavras")
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.imshow(wordcloud_all, interpolation="bilinear")
    ax.axis("off")
    st.pyplot(fig)

    # Show the 20 words that appear more frequently
    st.subheader("20 Palavras Mais Frequentes")

    # Count the frequency of each word
    word_counts = Counter(words_filtered).most_common(20)
    word_counts_term = Counter(words_filtered_term).most_common(20)
    # Create a dataframe to show the data
    word_freq_df = pd.DataFrame(word_counts, columns=["Palavra", "Frequência"])
    word_freq_term_df = pd.DataFrame(word_counts_term, columns=["Palavra", "Frequência"])

    # Create columns side by side
    col1, col2 = st.columns(2)

    # Exibe the table with most frequent words
    with col1:
        st.write("### Palavras mais frequentes no texto")
        st.dataframe(word_freq_df, height=300)  # Enable scroll for the table)

    with col2:
        st.write("### Palavras mais frequentes no utm term")
        st.dataframe(word_freq_term_df, height=300)  # Enable scroll for the table)
    # st.write("### Palavras mais frequentes no texto")
    # st.dataframe(word_freq_df, 

else:
    st.write("Por favor, insira a senha correta para acessar o conteúdo.")