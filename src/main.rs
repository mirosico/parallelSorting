use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;
use rand::Rng;

fn counting_sort(arr: &mut [usize]) {
    let max = match arr.iter().max() {
        Some(&max) => max,
        None => return,
    };

    let mut count = vec![0; max + 1];
    let mut output = vec![0; arr.len()];

    for &number in arr.iter() {
        count[number] += 1;
    }

    for i in 1..count.len() {
        count[i] += count[i - 1];
    }

    for &number in arr.iter().rev() {
        output[count[number] - 1] = number;
        count[number] -= 1;
    }

    arr.copy_from_slice(&output);
}

fn radix_sort(arr: &mut [usize]) {
    let max_number = match arr.iter().max() {
        Some(&max) => max,
        None => return,
    };
    let max_digits = (max_number as f64).log(10.0) as usize + 1;

    let mut output = vec![0; arr.len()];
    let mut count;
    let mut significant_digit = 1;

    for _ in 0..max_digits {
        count = vec![0; 10];

        for &number in arr.iter() {
            let digit = (number / significant_digit) % 10;
            count[digit] += 1;
        }

        for i in 1..10 {
            count[i] += count[i - 1];
        }

        for &number in arr.iter().rev() {
            let digit = (number / significant_digit) % 10;
            output[count[digit] - 1] = number;
            count[digit] -= 1;
        }

        arr.copy_from_slice(&output);
        significant_digit *= 10;
    }
}

fn merge(arr1: &[usize], arr2: &[usize]) -> Vec<usize> {
    let mut merged = Vec::with_capacity(arr1.len() + arr2.len());
    let (mut i, mut j) = (0, 0);

    while i < arr1.len() && j < arr2.len() {
        if arr1[i] <= arr2[j] {
            merged.push(arr1[i]);
            i += 1;
        } else {
            merged.push(arr2[j]);
            j += 1;
        }
    }

    while i < arr1.len() {
        merged.push(arr1[i]);
        i += 1;
    }

    while j < arr2.len() {
        merged.push(arr2[j]);
        j += 1;
    }

    merged
}


fn parallel_tree_based_merge(sorted_subarrays: &[Arc<Mutex<Vec<usize>>>]) -> Arc<Mutex<Vec<usize>>> {
    let len = sorted_subarrays.len();

    if len == 1 {
        return Arc::clone(&sorted_subarrays[0]);
    }

    let mut handles = vec![];
    let mut new_subarrays = vec![];

    for i in (0..sorted_subarrays.len()).step_by(2) {
        let arr1 = Arc::clone(&sorted_subarrays[i]);
        let arr2 = if i + 1 < len {
            Arc::clone(&sorted_subarrays[i + 1])
        } else {
            new_subarrays.push(Arc::clone(&sorted_subarrays[i]));
            break;
        };

        let handle = thread::spawn(move || {
            let arr1 = arr1.lock().unwrap();
            let arr2 = arr2.lock().unwrap();
            let merged = merge(&arr1, &arr2);
            return merged;
        });

        handles.push(handle);
    }

    for handle in handles {
        let merged = handle.join().unwrap();
        let merged_arc = Arc::new(Mutex::new(merged));
        new_subarrays.push(merged_arc);
    }

    parallel_tree_based_merge(&new_subarrays)
}


fn main() {
    let n = 10;
    let num_threads = 10;


    let mut arr: Vec<usize> = (0..n).map(|_| rand::thread_rng().gen_range(0..100)).collect();
    //let arr: Vec<usize> = vec![1; n];


    let arr_chunks = arr.chunks_mut(n / num_threads).map(|chunk| Arc::new(Mutex::new(chunk.to_vec()))).collect::<Vec<_>>();

    let start = Instant::now();

    let mut handles = vec![];
    for i in 0..num_threads {
        let arr_chunk = Arc::clone(&arr_chunks[i]);

        let handle = thread::spawn(move || {
            let mut data = arr_chunk.lock().unwrap();
            counting_sort(&mut data);
            //radix_sort(&mut data);
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }


    let sorted_array_arc = parallel_tree_based_merge(&arr_chunks);
    let sorted_array = sorted_array_arc.lock().unwrap();

    let duration = start.elapsed();

    println!("Array length: {:?}", n);
    println!("Num of threads: {:?}", num_threads);

    println!("Time taken: {:?}", duration);

    //println!("Array: {:?}", sorted_array);
}
