import ollama
import pandas as pd
import time
import tqdm

df = pd.read_excel('bsky_posts_labeled.xlsx')

# data cleaning
df = df[df['Text'].notna()]  # Remove rows where Text column has null/NA values
df = df[df['Text'].str.strip() != '']  # Remove rows where Text column is empty or only whitespace
df = df.drop_duplicates(subset=['Text'])  # Remove duplicate posts
df['ID'] = df.index + 1 # Overwrite 'ID' column with sequential numbers for unique post IDs
# print(df.head())
# print(df.shape)

# # test ollama
# test_text = df['Text'].iloc[0]
# response = ollama.chat(
#     model="llama3.2:latest", 
#     messages=[
#         {"role": 'system',
#          "content": "You are a sentiment analyzer. Respond with only: POSITIVE, NEGATIVE, or NEUTRAL."
#          },
#          {"role": "user", 
#           "content": f"Text: {test_text}"
#           }
#     ]
# )

# response2 = ollama.generate(
#     model="llama3.2:latest",
#     prompt=f"You are a sentiment analyzer. Respond with only: POSITIVE, NEGATIVE, or NEUTRAL. Text: {test_text}"
# )
# print(f'Chat {response}\n')
# print(f'Generate {response2}')

# SENTIMENT ANALYSIS
def llama_sentiment_analysis(text):
    max_retries = 3
    retry_delay = 1

    for attempt in range(max_retries):
        try:
            """Prompt llama3.2:latest to analyze sentiment of text"""    
            response = ollama.generate(
                model="llama3.2:latest",
                prompt=f"You are a sentiment analyzer. Respond with only: POSITIVE, NEGATIVE, or NEUTRAL. Text: {text}"
            )
            return response
        except Exception as e:
            """If attempt fails, retry up to max_retries times"""
            if attempt < max_retries - 1:
                print(f"Attempt {attempt + 1} failed for llama3.2:latest: {str(e)}. Retrying...")
                time.sleep(retry_delay)
            else:
                print(f"All attempts failed for llama3.2:latest: {str(e)}")
                return {
                    'response': 'ERROR',
                    'total_duration': 0
                }

def qwen_sentiment_analysis(text):
    max_retries = 3
    retry_delay = 1

    for attempt in range(max_retries):
        try:
            """Prompt qwen2.5:3b to analyze sentiment of text"""
            response = ollama.generate(
                model="qwen2.5:3b",
                prompt=f"You are a sentiment analyzer. Respond with only: POSITIVE, NEGATIVE, or NEUTRAL. Text: {text}"
            )
            return response
        except Exception as e:
            """If attempt fails, retry up to max_retries times"""
            if attempt < max_retries - 1: # If not last attempt
                print(f"Attempt {attempt + 1} failed for qwen2.5:3b: {str(e)}. Retrying...")
                time.sleep(retry_delay) 
            else:
                print(f"All attempts failed for qwen2.5:3b: {str(e)}")
                return {
                    'response': 'ERROR',
                    'total_duration': 0
                } 

llama_sentiment = []
qwen_sentiment = []
llama_total_duration = []
qwen_total_duration = []

start_time = time.time() 

for post in tqdm.tqdm(df['Text'], desc='Processing posts...', unit='post'): # Iterate through each post
    llama_response = llama_sentiment_analysis(post)
    qwen_response = qwen_sentiment_analysis(post)

    llama_sentiment.append(llama_response['response'])
    qwen_sentiment.append(qwen_response['response']) 

    llama_total_duration.append(llama_response['total_duration'])
    qwen_total_duration.append(qwen_response['total_duration'])

    time.sleep(0.5)

end_time = time.time() 
total_duration = end_time - start_time # Calculate total duration of sentiment analysis
print(f'\nTotal duration: {total_duration} seconds') 

"""Add sentiment analysis results to dataframe"""
df['llama_sentiment'] = llama_sentiment
df['qwen_sentiment'] = qwen_sentiment
df['llama_total_duration'] = llama_total_duration
df['qwen_total_duration'] = qwen_total_duration

df.to_excel('sentiment_analysis.xlsx', index=False) # Save dataframe to excel

    