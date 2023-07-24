# Copyright 2023 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# Create service account if does not exist.

SERVICE_ACCOUNT=$1
SERVICE_ACCOUNT_ID=$2
gcloud iam service-accounts create $SERVICE_ACCOUNT --description="$SERVICE_ACCOUNT" --display-name="$SERVICE_ACCOUNT" &>> output.txt
if [ $? -eq 1 ]; then
  if grep "already exists within project" output.txt; then
    echo "Service account exist."
  else
    exit 1
  fi
fi
