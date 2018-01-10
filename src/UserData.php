<?php


namespace DaKine;


use Aws\S3\S3Client;
use Carbon\Carbon;
use DaKine\Model\ArchiveUser;
use DaKine\Model\User;
use Illuminate\Database\Capsule\Manager as DB;
use Illuminate\Support\Facades\Storage;
use League\Csv\Writer;
use SplTempFileObject;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class UserData extends Command
{
    /**
     *
     */


    public function configure()
    {
        $this->setName('user-data')
            ->setDescription('Create report to csv')
            ->addArgument('toDate', InputArgument::REQUIRED, 'date where report ends');
    }


    public function execute(InputInterface $input, OutputInterface $output)
    {

        $date = Carbon::parse($input->getArgument('toDate'))->toDateString();

        $query = User::whereNotNull('email')->whereNotNull(DB::raw('date(users.'.env('BRAINLABS_SUBSCRIBED_ON', 'subscribed_on').')'))
            ->where(DB::raw('date(users.'.env('BRAINLABS_SUBSCRIBED_ON', 'subscribed_on').')'),'!=', '0000-00-00 00:00:00')
            ->where(DB::raw('date(users.'.env('BRAINLABS_SUBSCRIBED_ON', 'subscribed_on').')'), '<=', $date);

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


            $archiveData = ArchiveUser::select('email', env('BRAINLABS_SUBSCRIBED_ON', 'subscribed_on'),
                env('BRAINLABS_UNSUBSCRIBED_ON', 'unsubscribed_on'), 'refunded')
                ->where(env('BRAINLABS_SUBSCRIBED_ON', 'subscribed_on'), '<=', $date)
                ->get();

            $this->populateCsv($archiveData, $csv);
        }

        $s3 = new S3Client([
            'version' => 'latest',
            'region'  => env('S3_REGION', 'us-east-1'),
            'credentials' => [
                'key' => env('S3_KEY'),
                'secret' => env('S3_SECRET')
            ]
        ]);

        $key =  $date.'_'.env('BRAINLABS_SERVICE_NAME', 'plustelecom_service').'_dump.csv';

        $s3->putObject([
            'ACL' => 'public-read',
            'Body' => (string) $csv,
            'Bucket' => env('S3_BUCKET'),
            'ContentType' => 'text/plain',
            'Key' => $key
        ]);

        $url = $s3->getObjectUrl(env('S3_BUCKET'), $key);

        $output->writeln($url);



//        Storage::disk('adwords_s3')->put($date.'_'.env('BRAINLABS_SERVICE_NAME', 'plustelecom_service').'_dump.csv', (string) $csv, 'public');
//
//        dd('test');
//
//        $url = Storage::disk('adwords_s3')->url($date.'_'.env('BRAINLABS_SERVICE_NAME', 'plustelecom_service').'_dump.csv');
//
//        $this->line($url);


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





























