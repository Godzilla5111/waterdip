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

from abc import ABC, abstractmethod

from waterdip.core.metrics.base import MongoMetric
from waterdip.core.monitors.models import MonitorCondition


class MonitorEvaluator(ABC):
    """ """

    def __init__(self, monitor_condition: MonitorCondition, metric: MongoMetric):
        self.monitor_condition = monitor_condition
        self.metric = metric

    @abstractmethod
    def evaluate(self, **kwargs) -> bool:
        pass
