
# 
# Copy the index from SOURCE to TARGET
# 
# Requirement:
# 		*	Source index and Target index should have same mapping.
# 

require_relative 'index_transfer'
require_relative 'my_time'

def copy_index
	size = get_index_size(SOURCE)
	no_of_batches = size/BATCH_SIZE

	time = MyTime.new
	puts "Program starts..."

	(0..no_of_batches).each do |batch_no|
		read_query = {}
		read_query[:size] = BATCH_SIZE
		read_query[:from] = batch_no*BATCH_SIZE
		source_extension = get_extension SOURCE
		res = post_es_response( SOURCE[:host], SOURCE[:port], source_extension, read_query.to_json )
		begin
			res = JSON.parse(res.body)
		rescue
			raise "Batch size is too high to parse."
		end

		body = []
		data = res["hits"]["hits"].map { |t| t["_source"] }
		data.each do |d|
			doc = {"index" => {"_index" => TARGET[:index_name],"_type" => TARGET[:type_name],"_id" => "#{d['id']}_#{d['type']}_copy4", "data" => d}}
			body.push(doc)
		end

		target_client = get_client TARGET
		response = target_client.bulk body: body
		if response[:errors] == true
      		raise "batch_no. #{batch_no} -- index with errors as #{(response["items"].map{|t| t["index"]["error"]}.compact)}"
    	end
    	puts "Copied batch no. #{batch_no} with #{BATCH_SIZE} docs from #{SOURCE[:host]}-(index_name: #{SOURCE[:index_name]}) to #{TARGET[:host]}-(index_name: #{TARGET[:index_name]})"

    	time.remaining_time((batch_no+1)*BATCH_SIZE, size) if batch_no%10 == 0
	end
	puts "====================================================================================================================================="
	puts "Index Sucessfully Copied. -- from #{SOURCE[:host]}-#{SOURCE[:index_name]} to #{TARGET[:host]}-#{TARGET[:index_name]}"
	time.time_taken
end
copy_index

