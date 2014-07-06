1: test

test passed under ruby-1.9.2-p320, ruote: 2.3.0.1, rails:3.2.8

suppose you cloned two project at the same dir.

ruote/
ruote-ar/

cd the ruote directory and run the test

create database
mysql -u root < setup.sql

run tests
RUOTE_STORAGE=ar ruby test/functional/storage.rb
