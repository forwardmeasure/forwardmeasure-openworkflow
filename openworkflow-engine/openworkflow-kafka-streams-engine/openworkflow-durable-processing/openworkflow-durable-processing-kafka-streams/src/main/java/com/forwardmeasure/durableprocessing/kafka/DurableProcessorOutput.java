package com.forwardmeasure.durableprocessing.kafka;

import com.forwardmeasure.durableprocessing.api.DurableCommandOutcome;
import java.util.Objects;

sealed interface DurableProcessorOutput<C, E, O> {
  record History<C, E, O>(E event) implements DurableProcessorOutput<C, E, O> {
    public History {
      Objects.requireNonNull(event, "event");
    }
  }

  record Command<C, E, O>(C command) implements DurableProcessorOutput<C, E, O> {
    public Command {
      Objects.requireNonNull(command, "command");
    }
  }

  record Outbox<C, E, O>(O value) implements DurableProcessorOutput<C, E, O> {
    public Outbox {
      Objects.requireNonNull(value, "value");
    }
  }

  record Rejected<C, E, O>(DurableDeadLetter deadLetter)
      implements DurableProcessorOutput<C, E, O> {
    public Rejected {
      Objects.requireNonNull(deadLetter, "deadLetter");
    }
  }

  record Outcome<C, E, O>(DurableCommandOutcome outcome)
      implements DurableProcessorOutput<C, E, O> {
    public Outcome {
      Objects.requireNonNull(outcome, "outcome");
    }
  }
}
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at https://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
