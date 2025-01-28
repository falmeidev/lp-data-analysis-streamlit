import os 
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from collections import Counter
import re
import plotly.express as px

# Get the data via API from BigQuery
@st.cache_data
def get_data():
    # extract credentials from secret
    service_account_info = {
        "type": st.secrets["gcp_service_account"]["type"],
        "project_id": st.secrets["gcp_service_account"]["project_id"],
        "private_key_id": st.secrets["gcp_service_account"]["private_key_id"],
        "private_key": st.secrets["gcp_service_account"]["private_key"],
        "client_email": st.secrets["gcp_service_account"]["client_email"],
        "client_id": st.secrets["gcp_service_account"]["client_id"],
        "auth_uri": st.secrets["gcp_service_account"]["auth_uri"],
        "token_uri": st.secrets["gcp_service_account"]["token_uri"],
        "auth_provider_x509_cert_url": st.secrets["gcp_service_account"]["auth_provider_x509_cert_url"],
        "client_x509_cert_url": st.secrets["gcp_service_account"]["client_x509_cert_url"],
    }
    # create the credentials
    credentials = Credentials.from_service_account_info(service_account_info)
    # configure the client
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
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
        user_pseudo_id
    FROM 
        `orbital-station-6a5ab.analytics_308291533.events_*`,
        UNNEST(event_params) AS event_params 
    WHERE PARSE_DATE('%Y%m%d', event_date) >= "2024-12-12"
        AND event_params.value.string_value LIKE "%produtos.orbital.company%"
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

# Initialize the state of the session
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

# Function to check the password
if not st.session_state.authenticated:
    password = st.text_input("Digite a senha para acessar o dashboard:", type="password")
    if password == st.secrets["credentials"]["password"]:
        st.session_state.authenticated = True
        st.success("Senha correta! Acessando o dashboard...")
    elif password != "":
        st.error("Senha incorreta. Tente novamente.")


# Try to access the application with the given password
if st.session_state.authenticated:

    # Title of the dashboard
    st.header("Landing Page Data Analysis - Orbital")

    # get the data
    data = get_data()

    # Delete the registers with 'user_pseudo_id' filled as 'unknown' registros onde `user_pseudo_id` é "unknown"
    data = data[data["user_pseudo_id"] != "unknown"]

    # Select the unique user_pseudo_id and replace by a new identifier
    unique_users = data["user_pseudo_id"].unique()
    user_id_map = {user: idx for idx, user in enumerate(unique_users, start=1000)}

    # Substituir user_pseudo_id por user_id
    data["user_id"] = data["user_pseudo_id"].map(user_id_map)

    # Show a sample of the data
    # st.write("### Dados Carregados")
    # st.write("Registros com `user_pseudo_id = 'unknown'` foram eliminados.")
    # st.dataframe(data[['event_date','event_timestamp','user_id','event_name','utm_term','frase']], height=300)  # Enable the scroll with fixed height

    # Apply filters

    st.sidebar.header("Filtros")

    # Filter by "event_name"
    st.sidebar.subheader("Filtrar por Event Name")
    st.sidebar.write("Por padrão, os eventos selecionados são:")
    default_events = ['first_visit', 'user_engagement', 'envio_leads_leadster']
    for event in default_events:
        st.sidebar.markdown(f"- {event}")
    event_names = data["event_name"].unique()
    selected_events = st.sidebar.multiselect("Selecione um ou mais eventos para análise:", ["Todos","Default"] + list(event_names), default="Default")
    #print(selected_events)

    # Filter by "utm_term"
    st.sidebar.subheader("Filtrar por UTM Term")
    utm_terms = data["utm_term"].unique()
    selected_terms = st.sidebar.multiselect("Selecione um ou mais eventos para análise:", ["Todos"] + list(utm_terms), default="Todos")
    #print(selected_terms)

    # Filter by "Period"
    st.sidebar.subheader("Filtrar por período")
    initial_date = data["event_date"].min() 
    final_date =  data["event_date"].max()
    date_interval = st.sidebar.slider("Selecione o período", 
                                      min_value=initial_date, 
                                      max_value= final_date, 
                                      value=(initial_date,final_date))


    # Apply the filters by event_name and UTM term
    if "Todos" in selected_events and "Todos" in selected_terms:
        filtered_data = data
    elif "Default" in selected_events and "Todos" in selected_terms: 
        filtered_data = data[data["event_name"].isin(default_events)]
        #print(filtered_data[["event_name"]].drop_duplicates())
    elif "Todos" in selected_events and "Todos" not in selected_terms:
        filtered_data = data[data["utm_term"].isin(selected_terms)]
    elif "Todos" not in selected_events and "Default" not in selected_events and "Todos" in selected_terms:
        filtered_data = data[data["event_name"].isin(selected_events)]
        #print(filtered_data[["event_name"]].drop_duplicates())
    else:
        filtered_data = data[(data["event_name"].isin(selected_events)) & (data["utm_term"].isin(selected_terms))]
    
    # Apply the period filter
    filtered_data = filtered_data[(filtered_data["event_date"] >= date_interval[0]) & (filtered_data["event_date"] <= date_interval[1])] 
    print(filtered_data["event_date"].min())
    print(filtered_data["event_date"].max())
    filtered_data.head()

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
    indicators_data = data[(data["event_date"] >= date_interval[0]) & (data["event_date"] <= date_interval[1])] 
    valores = [
        indicators_data[indicators_data["event_name"] == evento]["user_pseudo_id"].nunique()
        for evento in etapas.keys()
    ]

    # Show the cards side by side
    colunas = st.columns(len(etapas))  # Create a column for each card
    for i, coluna in enumerate(colunas):
        with coluna:
            st.metric(label=etapas[list(etapas.keys())[i]], value=valores[i])

    st.write(f"### Dados para o evento: {selected_events}")
    st.dataframe(filtered_data[['event_date','user_id','event_name','utm_term','frase']].sort_values("event_date"), height=300)  # Enable scroll for the table

    # Create the wordcloud
    st.subheader("Nuvem de Palavras")

    st.write("Palavras com ao menos 4 dígitos, e que aparecem na frase do evento selecionado.")

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
        st.write("Palavras mais frequentes no texto")
        st.dataframe(word_freq_df, height=300)  # Enable scroll for the table)

    with col2:
        st.write("Palavras mais frequentes no utm term")
        st.dataframe(word_freq_term_df, height=300)  # Enable scroll for the table)
    # st.write("### Palavras mais frequentes no texto")
    # st.dataframe(word_freq_df)

    # Show the performance of the landing page
    st.subheader("Desempenho da LP")

    # Count unique customers by event and date
    aggregated_data = (
        filtered_data.groupby(["event_date", "event_name"])["user_pseudo_id"]
        .nunique()
        .reset_index()
        .rename(columns={"user_pseudo_id": "unique_users"})
    )

    # Gráfico de linhas
    if not aggregated_data.empty:
        fig = px.line(
            aggregated_data,
            x="event_date",
            y="unique_users",
            color="event_name",
            title="Clientes únicos por evento ao longo do tempo",
            labels={"unique_users": "Clientes Únicos", "event_date": "Data"},
    )
    st.plotly_chart(fig)

    # Show the UTM term performance in generating the phrase
    st.subheader("UTM Term versus Frase")
    st.dataframe(filtered_data1[["frase","utm_term"]].drop_duplicates())
else:
    st.write("Aguardando inserção da senha correta para acessar o conteúdo...")