from pathlib import Path
import json
import aiofiles
import re
import asyncio
from openai import OpenAI
from loguru import logger
import copy
from Agent import ZhiPuAgent, MoonshotAgent, OpenAIChatAgent
import tqdm
import os
from asyncio import Queue
from utli import try_parse_json_object

moonshot_api_key = os.getenv("MOONSHOT_API_KEY")
openai_api_key = os.getenv("OPENAI_API_KEY")
zhipu_api_key = os.getenv("ZHIPU_API_KEY")
moonshot_agent = MoonshotAgent(api_key=moonshot_api_key, model="moonshot-v1-8k")
longer_moonshot_agent = MoonshotAgent(api_key=moonshot_api_key,
                                      model="moonshot-v1-32k")
# proxy = "http://127.0.0.1:7890"
proxy = None
openai_agent = OpenAIChatAgent(api_key=openai_api_key, model="gpt-4o",
                               proxy=proxy)
prompt = r"""
### Task Overview:
You are a financial analyst with expertise in data processing. Your task is to extract company-related **downgrade** information from provided HTML content. Specifically, extract details such as the **company name**, **publication date**, **product names**, and the corresponding **rating changes**. If a company has multiple products, list them accordingly. Subsidiaries should be grouped under their parent company.

### Key Requirements:
- **Objective**: Extract only **confirmed downgrades** that have **already occurred**. Do not include potential downgrades (e.g., "possible downgrade") or those that have not been finalized.

- **Outlook Changes**: If the content only mentions changes in the **outlook** without specifying a confirmed rating downgrade, consider it as no actual rating change.

- **Output Format**: Return the results in a JSON-compatible Python dictionary. If no valid downgrade information is found, return an **empty dictionary**. Follow this structure:

```json
{
    "Company Name": "Company Name",
    "Has New Rating": true or false,
    "Reason": "Leave blank if a new rating is provided; otherwise, explain why the new rating is unknown",
    "Publication Date": "Publication Date",
    "Product Ratings": [
        {
            "Product Name": "Product Name",
            "Rating Change": {
                "Old Rating": "Old rating (if not explicitly stated, mark as null. If the rating is identified for the first time, consider the old rating as null)",
                "New Rating": "New rating (if unavailable, mark as Unknown)",
                "Change Reason": "If a new rating is provided, explain the reason for the change; if not, leave blank",
                "Raw Content": "Provide raw content if a new rating is given; otherwise, leave blank",
                "Is Subsidiary Product": true or false,
                "Subsidiary Name": "If the product belongs to a subsidiary, include the subsidiary name; otherwise, leave blank"
            }
        }
    ]
}
```

### Detailed Instructions:
1. **Extract Data**:
   - Parse the HTML content to extract the **company name**, **publication date**, **product names**, and **downgrade information**.
   - Clean the extracted text by removing any unnecessary characters such as `\"`, `\n`, or other symbols like `\x01`.

2. **Identify Rating Changes**:
   - Use regular expressions or other suitable methods to extract the **old rating** and **new rating**. Ratings should follow the "from X to Y" format.
   - If no new rating is provided, set the new rating as **Unknown** and explain the reason (e.g., "No new rating specified").
   - If the rating appears for the first time without a previous rating, mark the old rating as **null**.

3. **Handle Subsidiary Products**:
   - If a product belongs to a subsidiary, mark **Is Subsidiary Product** as `true` and provide the **Subsidiary Name**. Otherwise, set **Is Subsidiary Product** to `false` and leave the **Subsidiary Name** field blank.

4. **Handle Multiple Products**:
   - For each company, list all relevant products and their respective downgrade information. Group subsidiaries under their parent company.

5. **Filter for Confirmed Downgrades**:
   - Only extract actual downgrades that have **already occurred**. Ignore speculative or watchlist items like “possible downgrade.”
   - If the content mentions a change in **outlook** without detailing an actual downgrade, consider it as no rating change.

6. **Output**:
   - If valid downgrade information is present, return it in the specified format.
   - If **Has New Rating** is `false`, return an **empty dictionary**.
"""

clean_json_prompt = """
Please output a valid JSON object with can be transfer with json.loads() function.
"""
un_process_words = ['POSSIBLE DOWNGRADE', 'POSSIBLE FURTHER DOWNGRADE', 'POSSIBLE UPGRADE', 'POSSIBLE FURTHER UPGRADE',
                    'POSSIBLE STABLE', 'POSSIBLE FURTHER STABLE', 'POSSIBLE WATCH', 'POSSIBLE FURTHER WATCH',
                    'POSSIBLE REVIEW', 'POSSIBLE FURTHER REVIEW', 'POSSIBLE PLACEMENT', 'POSSIBLE FURTHER PLACEMENT',
                    'POSSIBLE WITHDRAW', 'POSSIBLE FURTHER WITHDRAW', 'POSSIBLE AFFIRM', 'POSSIBLE FURTHER AFFIRM',
                    'POSSIBLE OUTLOOK', 'POSSIBLE FURTHER OUTLOOK', 'POSSIBLE POSITIVE OUTLOOK',
                    'POSSIBLE FURTHER POSITIVE OUTLOOK', 'POSSIBLE NEGATIVE OUTLOOK',
                    'POSSIBLE FURTHER NEGATIVE OUTLOOK',
                    'POSSIBLE POSITIVE IMPLICATION', 'POSSIBLE FURTHER POSITIVE IMPLICATION',
                    'POSSIBLE NEGATIVE IMPLICATION', 'POSSIBLE FURTHER NEGATIVE IMPLICATION',
                    'POSSIBLE POSITIVE ACTION', 'POSSIBLE FURTHER POSITIVE ACTION', 'POSSIBLE NEGATIVE ACTION',
                    ]  # List of words to check for downgrade information


async def process_detail_file(detail_file: Path):
    async with aiofiles.open(detail_file, 'r') as f:
        data = json.loads(await f.read())
    # Check for downgrade information in a case-insensitive way in the researchPayload content
    downgrade = False
    is_error = False
    has_research_payload = False
    result = {}
    if 'researchPayload' in data:
        has_research_payload = True
        html_content = data['researchPayload'].get('html_content', '').lower()  # Convert content to lowercase
        # Published date
        report_publish_date = data['baseInfo'][0].get('published_date', None)
        title = data['baseInfo'][0].get('title', None)
        # Check for downgrade information in a case-insensitive way

        # if title is not None:
        #     uppercase_title = title.upper()
        #     for word in un_process_words:
        #         if word in uppercase_title:
        #             downgrade = True
        #             break
        # 使用正则表达式过滤html_content中的标签
        # 1. 去除<head>标签及其内容
        html_content = re.sub(r'<html.*</head>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        # 2. 去除<script>标签及其内容
        html_content = re.sub(r'<script.*</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        html_content = re.sub(r'"', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        # 2. 去除无用的换行和空格
        save_html_content = copy.deepcopy(html_content)
        html_content = re.sub(r'[\n|\r|"]', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        html_content = re.sub(r'\s+', ' ', html_content, flags=re.DOTALL | re.IGNORECASE)
        # 3. 去除所有html标签
        html_content = re.sub(r'<[^>]+>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        html_has_upgrade = 'upgrade' in html_content.lower()
        html_has_downgrade = 'downgrade' in html_content.lower()
        result['title'] = title
        result['report_publish_date'] = report_publish_date
        result['html_content'] = save_html_content
        if not (html_has_upgrade or html_has_downgrade):
            logger.info(f"No downgrade information found in {detail_file.name}")
            result['InvalidReason'] = "No downgrade information found in the HTML content."
        else:
            # 使用大模型进行预测
            # 1. 加载模型
            try:
                try:
                    if len(html_content) < 4096:
                        str_result = await openai_agent.get_response(input_message=html_content, prompt=prompt,
                                                                     temperature=0.0, max_tokens=4096)
                        # str_result = await moonshot_agent.get_response(input_message=html_content, prompt=prompt,
                        #                                              temperature=0.0, max_tokens=4096)
                    else:
                        str_result = await openai_agent.get_response(input_message=html_content, prompt=prompt,
                                                                     temperature=0.0, max_tokens=4096 * 4)
                        # str_result = await longer_moonshot_agent.get_response(input_message=html_content, prompt=prompt,
                        #                                              temperature=0.0, max_tokens=4096 * 4)
                except Exception as e:
                    if 'exceeded model token limit' in str(e):
                        logger.warning(
                            f"file: {detail_file} Exceeded model token limit, trying with longer model. the input message length is {len(html_content)}")
                        str_result = await longer_moonshot_agent.get_response(input_message=html_content, prompt=prompt,
                                                                              temperature=0.0, max_tokens=4096 * 4)
                    else:
                        logger.warning(f"Error processing {detail_file.name}: {e}")
                        raise e
                str_result = re.sub(r'', '', str_result)
                # str_result = str_result.replace(r'\"', '')
                # try:
                logger.info(
                    f"file name: {detail_file.name},json_result: {str_result},input_message_length: {len(html_content)},output_message_length: {len(str_result)}")
                _, json_result = try_parse_json_object(str_result)
                # except:
                #     logger.warning("Error decoding faulty json, attempting repair")
                #     logger.info(f"file name: {detail_file.name},str_result: {str_result}")
                #     clean_str_result = zhipu_agent.get_response(input_message=str_result,
                #                                                 prompt=clean_json_prompt,
                #                                                 temperature=0.0)
                #     _, json_result = try_parse_json_object(clean_str_result)
                # 2. 处理预测结果
                has_new_rating = json_result.get('Has New Rating', "False")
                if has_new_rating == "True" or has_new_rating is True:
                    downgrade = True
                if downgrade:
                    logger.info(f"Downgrade information found in {detail_file.name}")
                else:
                    logger.info(f"No downgrade information found in {detail_file.name}")
                    result['InvalidReason'] = "LLM model can't find downgrade information."
                for key, value in json_result.items():
                    result[key] = value
            except Exception as e:
                is_error = True
                logger.error(f"Error processing {detail_file.name}: {e}")
                result['InvalidReason'] = "Request to LLM model failed, the error message is: " + str(e)
    else:
        result['InvalidReason'] = "No researchPayload found in the JSON content."
    return result, downgrade, is_error, has_research_payload


async def save_json_file(file_path, data):
    async with aiofiles.open(file_path, 'w') as f:
        await f.write(json.dumps(data, indent=2))


async def producer(queue: Queue):
    data_path = Path('data')
    for year_data_path in data_path.iterdir():
        year = int(year_data_path.name)
        if year >= 2005:
            continue
        logger.info(f"Processing {year_data_path}")
        detail_data_path = year_data_path / 'detail'
        if detail_data_path.exists():
            detail_files = list(detail_data_path.glob('*.json'))
            processed_data_path = year_data_path / 'processed'
            processed_data_valid_path = processed_data_path / 'valid'
            processed_data_invalid_path = processed_data_path / 'invalid'
            processed_data_valid_path.mkdir(parents=True, exist_ok=True)
            processed_data_invalid_path.mkdir(parents=True, exist_ok=True)
            for detail_file in detail_files:
                processed_file_valid_path = processed_data_valid_path / detail_file.name
                processed_file_invalid_path = processed_data_invalid_path / detail_file.name
                if processed_file_valid_path.exists() or processed_file_invalid_path.exists():
                    logger.info(f"File {detail_file.name} already processed. Skipping...")
                    continue
                await queue.put((detail_file, processed_file_valid_path, processed_file_invalid_path))


async def consumer(queue: Queue):
    while True:
        detail_file, processed_file_valid_path, processed_file_invalid_path = await queue.get()
        logger.info(f"Processing {detail_file.name}")
        result, downgrade, is_error, has_research_payload = await process_detail_file(detail_file)
        if has_research_payload:
            if not is_error:
                if not downgrade:  # If no downgrade information is found, skip
                    processed_file = processed_file_invalid_path
                else:
                    processed_file = processed_file_valid_path
                await save_json_file(processed_file, result)
                logger.info(f"File {detail_file.name} processed successfully., saved to {processed_file}")
                await asyncio.sleep(0.2)  # Simulate I/O bound operation
            else:
                queue.put_nowait((detail_file, processed_file_valid_path, processed_file_invalid_path))
        else:
            logger.info(f"File {detail_file.name} has no researchPayload. Skipping...")
        queue.task_done()


async def main():
    queue = asyncio.Queue()
    producer_task = asyncio.create_task(producer(queue))
    consumer_tasks = [asyncio.create_task(consumer(queue)) for _ in range(10)]  # create 5 consumers
    logger.info("Waiting for all items in the queue to be processed...")
    await producer_task
    logger.info("Producer task completed.")
    await queue.join()  # Wait for all items in the queue to be processed
    for task in consumer_tasks:
        task.cancel()
    logger.info("All items in the queue have been processed.")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except RuntimeError as e:
        if "This event loop is already running" in str(e):
            print("Cannot use asyncio.run(), already in event loop. Running main() directly.")
            asyncio.ensure_future(main())  # If inside another event loop, schedule task
        else:
            raise e
