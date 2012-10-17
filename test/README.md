1: test

test passed under ruby-1.9.2-p320, ruote: 2.3.0.1, rails:3.2.8

suppose you cloned two project at the same dir.

ruote/
ruote-ar/

cd the ruote directory and run the test

"ruby test/functional/storage.rb -- --ar"

2: table

the table of ruote-ar is set to 'ruote_docs', it may will be changed as an option like ruote-sequel, todo.
and there is no create or check existent of the table, make sure the table is exist first.

table definition refer to https://github.com/jmettraux/ruote-sequel/blob/master/lib/ruote/sequel/storage.rb?#L46-L59
