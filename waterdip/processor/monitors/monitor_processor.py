#  Copyright 2022-present, the Waterdip Labs Pvt. Ltd.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import datetime
import uuid
from typing import Dict, List, Union
from uuid import UUID

from loguru import logger

from waterdip.core.commons.models import DataQualityMetric, DatasetType, MonitorType
from waterdip.core.metrics.data_metrics import CountEmptyHistogram
from waterdip.core.monitors.evaluators.data_quality import EmptyValueEvaluator
from waterdip.core.monitors.models import DataQualityBaseMonitorCondition
from waterdip.server.db.models.alerts import AlertDB, AlertIdentification, BaseAlertDB
from waterdip.server.db.models.datasets import BaseDatasetDB
from waterdip.server.db.mongodb import (
    MONGO_COLLECTION_EVENT_ROWS,
    MONGO_COLLECTION_MONITORS,
    MongodbBackend,
)
from waterdip.server.db.repositories.alert_repository import AlertRepository
from waterdip.server.db.repositories.dataset_repository import DatasetRepository
from waterdip.server.errors.base_errors import EntityNotFoundError


class MonitorProcessor:
    """
    Monitor Processor will use monitor evaluator to evaluate model
    metrics check if alerts needs to be generated ot not

    Attributes
    ----------
    monitor:
        Monitor data in json dictionary format
    """

    def __init__(
        self,
        monitor: Dict,
        mongodb_backend: MongodbBackend,
        alert_repo: AlertRepository,
        dataset_repo: DatasetRepository,
    ):
        self.monitor_type: MonitorType = MonitorType(monitor["monitor_type"])
        self._mongo_backend = mongodb_backend
        self._database = mongodb_backend.database
        self._alert_repo = alert_repo
        self._dataset_repo = dataset_repo
        self.monitor_id = monitor["monitor_id"]
        self._model_version_id = monitor["monitor_identification"]["model_version_id"]
        self.model_id = monitor["monitor_identification"]["model_id"]
        if self.monitor_type == MonitorType.DATA_QUALITY:
            self.monitor_condition = DataQualityBaseMonitorCondition(
                **monitor["monitor_condition"]
            )

    def _data_quality_processor(self) -> List[Dict]:
        """
        Processor for data quality monitors.
        Selects the Evaluator type based on evaluation_metric type
        """
        if self.monitor_condition.evaluation_metric == DataQualityMetric.EMPTY_VALUE:
            evaluator = EmptyValueEvaluator(
                monitor_condition=self.monitor_condition,
                metric=CountEmptyHistogram(
                    collection=self._database[MONGO_COLLECTION_EVENT_ROWS],
                    dataset_id=self._get_event_dataset().dataset_id,
                ),
            )
        else:
            raise NotImplementedError()

        return evaluator.evaluate()

    def _get_event_dataset(self) -> Union[BaseDatasetDB, None]:
        """
        Get event dataset for the model version id
        """
        event_dataset: List[BaseDatasetDB] = self._dataset_repo.find_datasets(
            filters={
                "model_version_id": self._model_version_id,
                "dataset_type": DatasetType.EVENT,
            }
        )
        if not event_dataset:
            raise EntityNotFoundError(
                type="event_dataset", name=str(self._model_version_id)
            )

        return event_dataset[0]

    def _create_alert(self, violation: Dict) -> AlertDB:
        alert = BaseAlertDB(
            monitor_id=self.monitor_id,
            model_id=self.model_id,
            alert_id=uuid.uuid4(),
            monitor_type=self.monitor_type,
            alert_identification=AlertIdentification(
                model_id=self.model_id, model_version_id=self._model_version_id
            ),
            created_at=datetime.datetime.utcnow(),
            violation=violation,
        )

        return self._alert_repo.insert_alert(alert=alert)

    def process(self) -> List[Dict]:
        """
        Process the monitor. Selects the processor type based on MonitorType
        """
        if self.monitor_type == MonitorType.DATA_QUALITY:
            violations = self._data_quality_processor()
        else:
            raise NotImplementedError()
        logger.info(
            f"evaluation done for Monitor ID [{self.monitor_id}] number of violations: [{len(violations)}]"
        )

        for violation in violations:
            agg_pipeline = [
                {
                    "$match": {
                        "alert_identification.model_version_id": self._model_version_id,
                        "violation.field": violation["dimension"],
                        "violation.focal_time_window": self.monitor_condition.evaluation_window,
                        "violation.max_threshold": violation["threshold"].value,
                    }
                }
            ]
            count = len(list(self._alert_repo.agg_alerts(agg_pipeline)))
            if count == 0:
                _violation = {
                    "field": violation["dimension"],
                    "max_threshold": violation["threshold"].value,
                    "model_version_id": self._model_version_id,
                    "focal_time_window": self.monitor_condition.evaluation_window,
                    "focal_value": violation["metric_value"],
                }
                self._create_alert(_violation)

        self._database[MONGO_COLLECTION_MONITORS].update_one(
            {"monitor_id": self.monitor_id},
            {"$set": {"last_run": datetime.datetime.utcnow()}},
        )
        return violations
