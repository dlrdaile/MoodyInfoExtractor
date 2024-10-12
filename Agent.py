from openai import OpenAI
import re
from utli import try_parse_json_object
from loguru import logger
import json
import aiohttp
# import httpx
import tqdm


class BaseAgent:
    def __init__(self, api_key, base_url, model):
        self.client = OpenAI(
            api_key=api_key,
            base_url=base_url,
        )
        self.base_url = base_url
        self.api_key = api_key
        self.model = model


class ZhiPuAgent(BaseAgent):
    PATTERN = re.compile(r"```(?:json\s+)?(\W.*?)```", re.DOTALL)
    GLM_JSON_RESPONSE_PREFIX = """You should always follow the instructions and output a valid JSON object.
    The structure of the JSON object you can found in the instructions, use {"answer": "$your_answer"} as the default structure
    if you are not sure about the structure.
    
    And you should always end the block with a "```" to indicate the end of the JSON object.
    
    <instructions>
    """

    GLM_JSON_RESPONSE_SUFFIX = """Output:
    </instructions>
    
    """

    def __init__(self, api_key, base_url="https://open.bigmodel.cn/api/paas/v4/", model="glm-4-plus"):
        super().__init__(api_key, base_url, model)

    def get_response(self, input_message, prompt, temperature=0.5):
        completion = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": f"{self.GLM_JSON_RESPONSE_PREFIX}{prompt}"},
                {"role": "user", "content": f"{input_message}{self.GLM_JSON_RESPONSE_SUFFIX}"}
            ],
            temperature=temperature,
        )
        result = completion.choices[0].message.content
        action_match = self.PATTERN.search(result)
        if action_match is not None:
            # json_text, result = try_parse_json_object(action_match.group(1).strip())
            result = action_match.group(1).strip()
            logger.info(f"json_text: {result}")
        return result


class MoonshotAgent(BaseAgent):
    def __init__(self, api_key, base_url="https://api.moonshot.cn/v1", model="moonshot-v1-8k"):
        super().__init__(api_key, base_url, model)
        self.header = header = {
            "Authorization": f"Bearer {api_key}",
        }

    async def get_response(self, input_message, prompt, temperature=0.5, max_tokens=4096):
        # completion = self.client.chat.completions.create(
        #     model=self.model,
        #     messages=[
        #         {"role": "system", "content": prompt},
        #         {"role": "user", "content": input_message}
        #     ],
        #     temperature=temperature,
        #     response_format={"type": "json_object"}
        # )
        # result = completion.choices[0].message.content
        # return result
        # async with httpx.AsyncClient() as client:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self.base_url}/chat/completions", headers=self.header, json={
                "model": self.model,
                "messages": [
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": input_message}
                ],
                "temperature": temperature,
                "max_tokens": max_tokens
            }) as response:
                if response.status != 200:
                    logger.error(f"response status: {response.status}, response text: {await response.text()}")
                    raise Exception(f"response status: {response.status}, response text: {await response.text()}")
                result = await response.json()
                return result["choices"][0]["message"]["content"]


class OpenAIChatAgent(BaseAgent):
    def __init__(self, api_key, base_url="https://api.openai.com/v1", model="gpt-4o-mini", proxy=None):
        super().__init__(api_key, base_url, model)
        self.header = header = {
            "Authorization": f"Bearer {api_key}",
        }
        self.proxy = proxy

    async def get_response(self, input_message, prompt, temperature=0.5, max_tokens=4096):
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self.base_url}/chat/completions", headers=self.header, json={
                "model": self.model,
                "messages": [
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": input_message}
                ],
                "temperature": temperature,
                "max_completion_tokens": max_tokens,
                "response_format": {"type": "json_object"}
            }, proxy=self.proxy
                                    ) as response:
                if response.status != 200:
                    logger.error(f"response status: {response.status}, response text: {await response.text()}")
                    raise Exception(f"response status: {response.status}, response text: {await response.text()}")
                result = await response.json()
                return result["choices"][0]["message"]["content"]
