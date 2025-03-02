import pandas as pd
from textblob import TextBlob
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import stanza
from transformers import pipeline
import pysentiment2
import tqdm

df = pd.read_excel('bsky_posts.xlsx')

# test_post = df['Text'][30]
# print("Test Post:")
# print(test_post)
'''def test_models(test_post):
    print("Test Post:")
    print(test_post)

    # test TextBlob
    blob = TextBlob(test_post)
    print("\nTextBlob Sentiment:")
    print(blob.sentiment.polarity)

    # test VaderSentiment
    analyzer = SentimentIntensityAnalyzer()
    sentiment_scores = analyzer.polarity_scores(test_post)
    print("\nVader Sentiment Scores:")
    print(sentiment_scores)

    # test stanza
    # stanza.download('en') # only need to run once
    nlp = stanza.Pipeline('en')
    doc = nlp(test_post)
    print("\nStanza Sentiment Analysis:")
    for sentence in doc.sentences:
        print(sentence.sentiment)

    # test Twitter-roBERTa-base
    print("\nTwitter-roBERTa-base Sentiment Analysis:")
    classifier = pipeline("sentiment-analysis", model="cardiffnlp/twitter-roberta-base-sentiment-latest")
    result = classifier(test_post)
    print(result[0]['label'])

    # test pysentiment2
    print("\nPysentiment2 Sentiment Analysis:")
    # Initialize the sentiment analyzers
    lm = pysentiment2.LM()  # Loughran-McDonald analyzer
    hiv4 = pysentiment2.HIV4()  # Harvard-IV analyzer

    # Tokenize and analyze the text
    lm_tokens = lm.tokenize(test_post)
    hiv4_tokens = hiv4.tokenize(test_post)

    # Get sentiment scores
    lm_scores = lm.get_score(lm_tokens)
    hiv4_scores = hiv4.get_score(hiv4_tokens)

    print("Loughran-McDonald Scores:")
    print(lm_scores)  # Returns dict with 'Positive', 'Negative', and 'Polarity' scores
    print("\nHarvard-IV Scores:")
    print(hiv4_scores)  # Returns dict with 'Positive', 'Negative', and 'Polarity' scores'''

# Load models once before the loop
print("Loading models...")
classifier = pipeline("sentiment-analysis", model="cardiffnlp/twitter-roberta-base-sentiment-latest")
nlp = stanza.Pipeline('en')
analyzer = SentimentIntensityAnalyzer()
lm = pysentiment2.LM()

# Initialize an empty list to store majority voted sentiment
majority_voted_sentiment = []

total_posts = len(df['Text'])
for idx, post in enumerate(tqdm.tqdm(df['Text'])):
    try:
        # Print progress every 100 posts
        if idx % 100 == 0:
            print(f"\nProcessing post {idx}/{total_posts} ({(idx/total_posts)*100:.2f}%)")
        
        # Convert post to string if it's not already
        post = str(post) if not isinstance(post, str) else post
        
        # TextBlob Analysis
        try:
            blob = TextBlob(post)
            textBlob_polarity = blob.sentiment.polarity
            if textBlob_polarity > 0.05:
                textBlob_sentiment = "POSITIVE"
            elif textBlob_polarity < -0.05:
                textBlob_sentiment = "NEGATIVE"
            else:
                textBlob_sentiment = "NEUTRAL"
        except Exception as e:
            print(f"TextBlob error for post {idx}: {post[:50]}... Error: {str(e)}")
            textBlob_sentiment = "NEUTRAL"
            textBlob_polarity = 0
        
        # VaderSentiment Analysis
        try:
            sentiment_scores = analyzer.polarity_scores(post)
            vader_score = sentiment_scores['compound']
            if vader_score > 0.05:
                vader_sentiment = "POSITIVE"
            elif vader_score < -0.05:
                vader_sentiment = "NEGATIVE"
            else:
                vader_sentiment = "NEUTRAL"
        except Exception as e:
            print(f"VADER error for post {idx}: {post[:50]}... Error: {str(e)}")
            vader_sentiment = "NEUTRAL"
            vader_score = 0
        
        # Pysentiment2 Analysis
        try:
            lm_tokens = lm.tokenize(post)
            lm_scores = lm.get_score(lm_tokens)
            lm_polarity = lm_scores['Polarity']
            if lm_polarity > 0.05:
                lm_sentiment = "POSITIVE"
            elif lm_polarity < -0.05:
                lm_sentiment = "NEGATIVE"
            else:
                lm_sentiment = "NEUTRAL"
        except Exception as e:
            print(f"Pysentiment2 error for post {idx}: {post[:50]}... Error: {str(e)}")
            lm_sentiment = "NEUTRAL"
            lm_polarity = 0
        
        # RoBERTa Analysis
        try:
            result = classifier(post)
            twitter_roberta_base_sentiment = result[0]['label'].upper()
            twitter_roberta_base_score = -1 if twitter_roberta_base_sentiment == "NEGATIVE" else 1 if twitter_roberta_base_sentiment == "POSITIVE" else 0
        except Exception as e:
            print(f"RoBERTa error for post {idx}: {post[:50]}... Error: {str(e)}")
            twitter_roberta_base_sentiment = "NEUTRAL"
            twitter_roberta_base_score = 0
        
        # Stanza Analysis
        try:
            doc = nlp(post)
            stanza_scores = [sentence.sentiment for sentence in doc.sentences]
            avg_stanza_score = sum(stanza_scores) / len(stanza_scores)
            stanza_normalized_score = (avg_stanza_score - 1)
            if avg_stanza_score > 1.05:
                stanza_sentiment = "POSITIVE"
            elif avg_stanza_score < 0.95:
                stanza_sentiment = "NEGATIVE"
            else:
                stanza_sentiment = "NEUTRAL"
        except Exception as e:
            print(f"Stanza error for post {idx}: {post[:50]}... Error: {str(e)}")
            stanza_sentiment = "NEUTRAL"
            stanza_normalized_score = 0

        # Save intermediate results every 100 posts
        if idx % 100 == 0:
            temp_df = df.iloc[:idx+1].copy()
            temp_df['majority_voted_sentiment'] = majority_voted_sentiment
            temp_df.to_excel('bsky_posts_labeled_temp.xlsx', index=False)

        # Majority voting
        labels = [textBlob_sentiment, vader_sentiment, lm_sentiment, twitter_roberta_base_sentiment, stanza_sentiment]
        scores = [textBlob_polarity, vader_score, lm_polarity, twitter_roberta_base_score, stanza_normalized_score]
        
        if labels.count("POSITIVE") >= 3:
            majority_voted_sentiment.append("POSITIVE")
        elif labels.count("NEGATIVE") >= 3:
            majority_voted_sentiment.append("NEGATIVE")
        elif labels.count("NEUTRAL") >= 3:
            majority_voted_sentiment.append("NEUTRAL")
        else:
            avg_score = sum(scores) / len(scores)
            if avg_score > 0.05:
                majority_voted_sentiment.append("POSITIVE")
            elif avg_score < -0.05:
                majority_voted_sentiment.append("NEGATIVE")
            else:
                majority_voted_sentiment.append("NEUTRAL")
                
    except Exception as e:
        print(f"Major error processing post {idx}: {post[:50]}... Error: {str(e)}")
        majority_voted_sentiment.append("NEUTRAL")

# Final save
if len(majority_voted_sentiment) == len(df):
    df['majority_voted_sentiment'] = majority_voted_sentiment
    df.to_excel('bsky_posts_labeled.xlsx', index=False)
else:
    print(f"Warning: Length mismatch. DataFrame: {len(df)}, Sentiment results: {len(majority_voted_sentiment)}")
