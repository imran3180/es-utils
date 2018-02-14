# 
# Replicate data of any index on same machice(TARGET) by REPLICATING_FACTOR
# Usage:	
# 	*	To benchmark on larger set of data, Even if you don't have sufficient data
# 

-----------------------------
Not working - Reading from same index and wrting to same index.!!!

require_relative 'index_transfer'
require_relative 'my_time'

REPLICATING_FACTOR = 4

def replicate_index
	size = get_index_size(TARGET)
	no_of_batches = size/BATCH_SIZE

	time = MyTime.new
	puts "Program starts..."

	(0..no_of_batches).each do |batch_no|
		read_query = {}
		read_query[:size] = BATCH_SIZE
		read_query[:from] = batch_no*BATCH_SIZE
		target_extension = get_extension TARGET
		res = post_es_response( TARGET[:host], TARGET[:port], target_extension, read_query.to_json )
		begin
			res = JSON.parse(res.body)
		rescue
			raise "Batch size is too high to parse."
		end
		body = []
		data = res["hits"]["hits"].map { |t| t["_source"] }
		data.each do |d|
			doc = []
			(1..REPLICATING_FACTOR).each do |copy_no|
				doc << {"index" => {"_index" => TARGET[:index_name],"_type" => TARGET[:type_name],"_id" => "#{d['id']}_#{d['type']}_copy#{copy_no}", "data" => d}}
			end
			body += doc
		end

		target_client = get_client TARGET
		response = target_client.bulk body: body
		if response[:errors] == true
      raise "batch_no. #{batch_no} -- index with errors as #{(response["items"].map{|t| t["index"]["error"]}.compact)}"
    end
    puts "Replicated #{REPLICATING_FACTOR} times: batch no. #{batch_no} with #{BATCH_SIZE} docs in #{TARGET[:host]}-(index_name: #{TARGET[:index_name]})"

    time.remaining_time((batch_no+1)*BATCH_SIZE*REPLICATING_FACTOR, size*REPLICATING_FACTOR) if batch_no%10 == 0

	end
	puts "====================================================================================================================================="
	puts "Index Sucessfully Replicated #{REPLICATING_FACTOR} times. -- in #{TARGET[:host]}-#{TARGET[:index_name]}"
	time.time_taken
end
replicate_index