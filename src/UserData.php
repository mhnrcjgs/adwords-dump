<?php


namespace Dakine;


use Carbon\Carbon;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Storage;
use League\Csv\Writer;
use SplTempFileObject;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class UserData extends Command
{


    public function configure()
    {
        $this->setName('data-to-csv')
            ->setDescription('Create report to csv')
            ->addArgument('toDate', InputArgument::REQUIRED, 'date where report ends');
    }


    public function execute(InputInterface $input, OutputInterface $output)
    {

        $date = $this->argument('toDate');

        if (!$date) {
            $date = Carbon::now()->subDay();
        }

        $date = Carbon::parse($date)->toDateString();

        $userModel = env('BRAINLABS_USER_MODEL', 'User');

        $userClass = new $userModel();

        $query = $userClass::where(DB::raw('date(users.'.env('BRAINLABS_SUBSCRIBED_ON', 'subscribed_on').')'), '<=', $date);

        $select = ['email', 'users.'.env('BRAINLABS_SUBSCRIBED_ON', 'subscribed_on'),
            'users.'.env('BRAINLABS_UNSUBSCRIBED_ON', 'unsubscribed_on')];

        if (env('BRAINLABS_HAS_REFUNDED_FIELD', false)) {
            $select[] = env('BRAINLABS_REFUNDED_FIELD', 'refunded');
        } else if (env('BRAINLABS_HAS_REFUNDS_TABLE', false)) {
            $select[] = DB::raw('IFNULL('.env('BRAINLABS_REFUNDS_TABLE', 'refunds').'.id, 0) as refunded');

            if (env('BRAINLABS_JOIN_WITH_MSISDN', false))
            {
                $query->leftJoin(env('BRAINLABS_REFUNDS_TABLE', 'refunds'), 'users.msisdn', '=', env('BRAINLABS_REFUNDS_TABLE', 'refunds').'.msisdn');
            } else {
                $query->leftJoin(env('BRAINLABS_REFUNDS_TABLE', 'refunds'), 'users.id', '=', env('BRAINLABS_REFUNDS_TABLE', 'refunds').'.user_id');
            }

        }

        $data = $query->select($select)->get();

        $csv = Writer::createFromFileObject(new SplTempFileObject());

        $csv->insertOne(['email', 'subscription']);

        $this->populateCsv($data, $csv);

        if (env('BRAINLABS_ARCHIVE_USER_TABLE', false)) {

            $archivedUserModel = env('BRAINLABS_ARCHIVE_USER_MODEL', 'ArchiveUser');

            $archivedUserClass = new $archivedUserModel();

            $archiveData = $archivedUserClass::select('email', env('BRAINLABS_SUBSCRIBED_ON', 'subscribed_on'),
                env('BRAINLABS_UNSUBSCRIBED_ON', 'unsubscribed_on'), 'refunded')
                ->where(env('BRAINLABS_SUBSCRIBED_ON', 'subscribed_on'), '<=', $date)
                ->get();

            $this->populateCsv($archiveData, $csv);
        }

        Storage::disk('adwords_s3')->put($date.'_'.env('BRAINLABS_SERVICE_NAME', 'plustelecom_service').'_dump.csv', (string) $csv, 'public');

        $url = Storage::disk('adwords_s3')->url($date.'_'.env('BRAINLABS_SERVICE_NAME', 'plustelecom_service').'_dump.csv');

        $this->line($url);


    }


    protected function populateCsv($data, $csv)
    {
        foreach ($data->toArray() as $value)
        {
            if (!$value['email'])
            {
                continue;
            }
            if ($value[env('BRAINLABS_SUBSCRIBED_ON', 'subscribed_on')])
            {
                $csv->insertOne([hash('sha256', $value['email']),1]);
            }
            if ($value[env('BRAINLABS_UNSUBSCRIBED_ON', 'unsubscribed_on')])
            {
                if (isset($value[env('BRAINLABS_REFUNDED_FIELD', 'refunded')])
                    && $value[env('BRAINLABS_REFUNDED_FIELD', 'refunded')]) {
                    $csv->insertOne([hash('sha256', $value['email']),2]);
                } else {
                    $csv->insertOne([hash('sha256', $value['email']),0]);
                }
            }

        }
    }

}





























