#!/usr/bin/env python
import json
from module_test_flow.schemas import InputSchema
from module_test_flow.utils import get_logger
from naptha_sdk.task import Task as NapthaTask
from naptha_sdk.utils import get_logger

logger = get_logger(__name__)


async def run(
    inputs: InputSchema,
    worker_nodes = None,
    orchestrator_node = None,
    flow_run = None,
    cfg: dict = None
):
    logger.info(f"Running with inputs {inputs.prompt}")
    logger.info(f"cfg: {cfg}")

    if worker_nodes is None:
        raise ValueError("worker_nodes must be provided")

    task1 = NapthaTask(
        name='module_test_flow_1',
        fn='module_test_template',
        worker_node=worker_nodes[0],
        orchestrator_node=orchestrator_node,
        flow_run=flow_run
    )

    response1 = await task1(
        prompt=inputs.prompt,
    )

    if isinstance(response1, str):
        response1 = json.loads(response1)
    elif isinstance(response1, list):
        response1 = response1[0]
        if isinstance(response1, str):
            response1 = json.loads(response1)
    else:
        raise ValueError("Invalid response type")

    logger.info(f"response1: {response1}")
    response1 = response1['modified_prompt']

    task2 = NapthaTask(
        name='module_test_flow_2',
        fn='module_test_template',
        worker_node=worker_nodes[0],
        orchestrator_node=orchestrator_node,
        flow_run=flow_run
    )

    response2 = await task2(
        prompt=response1,
    )

    logger.info(f"response2: {response2}")

    if isinstance(response2, str):  
        response2 = json.loads(response2)
    elif isinstance(response2, list):
        response2 = response2[0]
        if isinstance(response2, str):
            response2 = json.loads(response2)
    else:
        raise ValueError("Invalid response type")

    response2 = response2['modified_prompt']

    return response2