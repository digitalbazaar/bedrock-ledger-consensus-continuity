counter=0
while [ $? -eq 0 ]
do
echo 'Pass ==================' $counter
((++counter))
npm test
done
