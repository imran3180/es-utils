# 
# Make flat config index from the Buy Index
# 

require_relative 'index_transfer'
require_relative 'my_time'

FLAT_CONFIG_INDEX_KEYS = ["city_id", "collection_ids", "type" ,"has_offer", "has_slice_view", "location_coordinates", "combined_polygon_uuids", "polygon_uuids"]
FLAT_CONFIG_INDEX = {
	:host => "10.1.7.212",
	:port => "9200",
	:index_name => "flat_configs",
	:type_name => "flat_config"
}

def create_flat_config_index
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
		inventories = res["hits"]["hits"].map { |t| t["_source"] }
		
		flat_configs = []
		inventories.each do |inventory|
			inventory_data = inventory.select{ |t| FLAT_CONFIG_INDEX_KEYS.include?(t)}
			inventory_configs = inventory["inventory_configs"]
			inventory_configs.each do |inventory_config|
				result = [	inventory_data, 
										inventory["inventory_amenities"],
										get_inventory_config_data(inventory_config),
										get_contact_person_info_data(inventory["contact_persons_info"], inventory["type"])
									]
				result = result.inject(:merge)
				result["inventory_id"] = inventory["id"]
      	flat_configs << result
			end
		end

		flat_configs.each do |d|
			doc = {"index" => {"_index" => FLAT_CONFIG_INDEX[:index_name],"_type" => FLAT_CONFIG_INDEX[:type_name],"_id" => "#{d['id']}_#{d['type']}", "data" => d}}
			body.push(doc)
		end
		target_client = get_client FLAT_CONFIG_INDEX
		response = target_client.bulk body: body
		if response[:errors] == true
      raise "batch_no. #{batch_no} -- index with errors as #{(response["items"].map{|t| t["index"]["error"]}.compact)}"
    end
    puts "Copied batch no. #{batch_no} with #{BATCH_SIZE} docs from #{SOURCE[:host]}-(index_name: #{SOURCE[:index_name]}) to #{FLAT_CONFIG_INDEX[:host]}-(index_name: #{FLAT_CONFIG_INDEX[:index_name]})"

    time.remaining_time((batch_no+1)*BATCH_SIZE, size) if batch_no%10 == 0
	end
	puts "====================================================================================================================================="
	puts "Flat Config Index Created."
	time.time_taken

end

def get_inventory_config_data(inventory_config)
	config_data = inventory_config.slice("area", "number_of_toilets", "is_available", "price", "property_type_id", "apartment_type_id", "completion_date", "flat_config_id", "price_on_request", "facing", "seller")
  config_data["id"] = config_data["flat_config_id"]
  config_data.except!("flat_config_id")
  config_data
end

def get_contact_person_info_data(contact_persons_info, type)
	result = {}
	return result if contact_persons_info.nil?
  result["contact_person_id"] = contact_persons_info.map { |t| t['contact_person_id'] }.uniq
  # since we allow only devloper filter.
  result["uuid"] = contact_persons_info.detect{ |t| t['contact_person_id'] == 3 }['uuid'] if type == "project"
  result
end
create_flat_config_index