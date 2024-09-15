#!/usr/bin/env python
import json
from module_test_template.schemas import InputSchema
from module_test_template.utils import get_logger
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
        fn='napthaville_module',
        worker_node=worker_nodes[0],
        orchestrator_node=orchestrator_node,
        flow_run=flow_run
    )

    response1 = await task1(
        prompt=inputs.prompt,
    )

    response1 = json.loads(response1)
    response1 = response1['modified_prompt']

    task2 = NapthaTask(
        name='module_test_flow_2',
        fn='napthaville_module',
        worker_node=worker_nodes[1],
        orchestrator_node=orchestrator_node,
        flow_run=flow_run
    )

    response2 = await task2(
        prompt=response1,
    )

    response2 = json.loads(response2)
    response2 = response2['modified_prompt']

    return response2