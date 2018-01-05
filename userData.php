#! /usr/bin/env php


<?php


use Dakine\UserData;
use Symfony\Component\Console\Application;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

require 'vendor/autoload.php';

require 'config/database.php';

$app = new Application('Upload File', '1.0');

$app->add(new UserData);

$app->run();
