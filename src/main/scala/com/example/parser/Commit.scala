package com.example.parser

case class Commit(
                   author: String,
                   distinct: Boolean,
                   message: String,
                   sha: String,
                   url: String
                )
