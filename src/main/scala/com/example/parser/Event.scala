package com.example.parser

case class Event(
                  actor: Actor,
                  created_at: java.sql.Timestamp,
                  id: String,
                  org: String,
                  payload: Payload,
                  publicField: Boolean,
                  repo: Repo,
                  `type`:String
                )

