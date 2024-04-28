#include "sql/operator/aggregate_physical_operator.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

RC AggregatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  std::unique_ptr<PhysicalOperator> &child = children_[0];
  RC                                 rc    = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  trx_ = trx;

  return RC::SUCCESS;
}

RC AggregatePhysicalOperator::next()
{
    //alread
    if(result_tuple_.cell_num()>0)
    {
        return RC::RECORD_EOF;
    }

    RC rc = RC::SUCCESS;
    PhysicalOperator *oper = children_[0].get();


    std::vector<Value> result_cells(aggregations_.size());//请记得一定初始化容器以及不要盲目相信演示代码（8h的教训）
    int num = 0;
    while(RC::SUCCESS == (rc = oper->next())){
        // get tuple
        Tuple *tuple = oper->current_tuple();
        //du aggregate
        for(int cell_idx = 0;cell_idx<(int)aggregations_.size();cell_idx++){
            const AggrOp aggregation = aggregations_[cell_idx];

            Value cell;
            AttrType attr_type = AttrType::INTS;
            switch (aggregation)
            {
            case AggrOp::AGGR_SUM:
              rc = tuple->cell_at(cell_idx,cell);
              attr_type = cell.attr_type();
              if(attr_type == AttrType::INTS or attr_type == AttrType::FLOATS){
                result_cells[cell_idx].set_float(result_cells[cell_idx].get_float()+cell.get_float());
              }
              break;
            case AggrOp::AGGR_COUNT:
              rc = tuple->cell_at(cell_idx,cell);
              result_cells[cell_idx].set_int(result_cells[cell_idx].get_int() + 1); // Increment count by 1 for each tuple
              break;
            case AggrOp::AGGR_COUNT_ALL:
              rc = tuple->cell_at(cell_idx,cell);
              result_cells[cell_idx].set_int(result_cells[cell_idx].get_int() + 1); // Increment count by 1 for each tuple
              break;  
            case AggrOp::AGGR_MIN:
              rc = tuple->cell_at(cell_idx, cell);
              attr_type = cell.attr_type();
              switch(cell.compare(result_cells[cell_idx])) 
              {
                case 1:
                  break;
                case -1:
                  result_cells[cell_idx].set_value(cell); 
                  break;
                default:
                  break;
              }
              break;
            case AggrOp::AGGR_MAX:
                            rc = tuple->cell_at(cell_idx, cell);
              attr_type = cell.attr_type();
              switch(result_cells[cell_idx].compare(cell))
              {
                case 1:               
                  break;
                case -1:
                  result_cells[cell_idx].set_value(cell);
                  break;
                default:
                  break;
              }
              break;
            case AggrOp::AGGR_AVG:
              rc = tuple->cell_at(cell_idx, cell);
              attr_type = cell.attr_type();
              if(attr_type == AttrType::INTS){
                result_cells[cell_idx].set_float((result_cells[cell_idx].get_float()*num+cell.get_int())/(num+1));
                num++;
              }
              else if(attr_type == AttrType::FLOATS){      
                result_cells[cell_idx].set_float((result_cells[cell_idx].get_float()*num+cell.get_float())/(num+1));
                num++;
              }
              break;
            case AggrOp::AGGR_NONE:
                return RC::UNIMPLENMENT;

            default:
                return RC::UNIMPLENMENT;
            }
        }
    }
    if(rc == RC::RECORD_EOF){
        rc = RC::SUCCESS;
    }

    result_tuple_.set_cells(result_cells);

    return rc;
}


RC AggregatePhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}

void AggregatePhysicalOperator::add_aggregation(const AggrOp aggregation){
    aggregations_.push_back(aggregation);
 }

Tuple *AggregatePhysicalOperator::current_tuple()
{
  return &result_tuple_;
}