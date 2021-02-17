package com.example.parser

case class Event(
                  actor: Actor,
                  created_at: String,
                  id: BigInt,
                  org: String,
                  payload: Payload,
                  public: Boolean,
                  repo: Repo,
                  `type`:String
                )

